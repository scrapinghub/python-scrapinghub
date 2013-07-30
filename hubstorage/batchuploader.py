import time
import atexit
import socket
import logging
import warnings
from gzip import GzipFile
from itertools import count
import requests
from requests.compat import StringIO
from collections import deque
from Queue import Queue
from threading import Thread, Event
from .utils import xauth, iterqueue
from .serialization import jsonencode

logger = logging.getLogger('hubstorage.batchuploader')


class BatchUploader(object):

    retry_wait_time = 5.0
    worker_loop_delay = 1.0

    def __init__(self, client):
        self.client = client
        self.closed = False
        self._wait_event = Event()
        self._writers = deque()
        self._thread = Thread(target=self._worker)
        self._thread.daemon = True
        self._thread.start()
        atexit.register(self._atexit)

    def create_writer(self, url, start=0, auth=None, size=1000, interval=15,
                      qsize=None, content_encoding='identity',
                      maxitemsize=1024 ** 2):
        assert not self.closed, 'Can not create new writers when closed'
        auth = xauth(auth) or self.client.auth
        w = _BatchWriter(url=url,
                         auth=auth,
                         size=size,
                         start=start,
                         interval=interval,
                         qsize=qsize,
                         maxitemsize=maxitemsize,
                         content_encoding=content_encoding,
                         uploader=self)
        self._writers.append(w)
        return w

    def close(self, timeout=None):
        self.closed = True
        self.interrupt()
        self._thread.join(timeout)

    def interrupt(self):
        self._wait_event.set()

    def _atexit(self):
        if not self.closed:
            warnings.warn("%r not closed properly, some items may have been "
                          "lost!: %r" % (self, self._writers))

    def __del__(self):
        if not self.closed:
            self.close()

    def _interruptable_sleep(self):
        self._wait_event.wait(self.worker_loop_delay)
        self._wait_event.clear()

    def _worker(self):
        ctr = count()
        while True:
            if not self._writers:
                # Stop thread if closed and idle, but if open wait for writers
                if self.closed:
                    break
                self._interruptable_sleep()
                continue

            # Delay once all writers are processed
            if (ctr.next() % len(self._writers) == 0) and not self.closed:
                self._interruptable_sleep()

            # Get next writer to process
            w = self._writers.popleft()

            # Close open writers if uploader is closed
            if self.closed and not w.closed:
                w.close(block=False)

            # Checkpoint writer if eligible
            now = time.time()
            if w.itemsq.qsize() >= w.size or w.closed or w.flushme \
                    or w.checkpoint < now - w.interval:
                self._checkpoint(w)
                w.checkpoint = now

            # Re-queue pending or open writers
            if not (w.closed and w.itemsq.empty()):
                self._writers.append(w)

    def _checkpoint(self, w):
        q = w.itemsq
        qiter = iterqueue(q, w.size)
        data = self._content_encode(qiter, w)
        if qiter.count > 0:
            self._tryupload({
                'url': w.url,
                'offset': w.offset,
                'data': data,
                'auth': w.auth,
                'content-encoding': w.content_encoding,
            })
            w.offset += qiter.count
            for _ in xrange(qiter.count):
                q.task_done()

    def _content_encode(self, qiter, w):
        ce = w.content_encoding
        if ce == 'identity':
            return _encode_identity(qiter)
        elif ce == 'gzip':
            return _encode_gzip(qiter)
        else:
            raise ValueError('Writer using unknown content encoding: %s' % ce)

    def _tryupload(self, batch):
        # TODO: Implements exponential backoff and a global timeout limit
        while True:
            try:
                self._upload(batch)
                break
            except (socket.error, requests.RequestException) as e:
                if isinstance(e, requests.HTTPError):
                    r = e.response
                    msg = "[HTTP error %d] %s" % (r.status_code, r.text.rstrip())
                else:
                    msg = str(e)
                logger.warning("Failed writing data %s: %s", batch['url'], msg)
                time.sleep(self.retry_wait_time)

    def _upload(self, batch):
        params = {'start': batch['offset']}
        headers = {'content-encoding': batch['content-encoding']}
        self.client.session.request(method='POST',
                                    url=batch['url'],
                                    data=batch['data'],
                                    auth=batch['auth'],
                                    timeout=self.client.connection_timeout,
                                    params=params,
                                    headers=headers)


class ItemTooLarge(ValueError):
    """Raised when a serialized item is greater than 1MB"""


class _BatchWriter(object):

    def __init__(self, url, start, auth, size, interval, qsize,
                 maxitemsize, content_encoding, uploader):
        self.url = url
        self.offset = start
        self._nextid = count(start)
        self.auth = auth
        self.size = size
        self.interval = interval
        self.maxitemsize = maxitemsize
        self.content_encoding = content_encoding
        self.checkpoint = time.time()
        self.itemsq = Queue(size * 2 if qsize is None else qsize)
        self.closed = False
        self.flushme = False
        self.uploader = uploader

    def write(self, item):
        assert not self.closed, 'attempting writes to a closed writer'
        serialized = jsonencode(item)
        if len(serialized) > self.maxitemsize:
            raise ItemTooLarge('item exceeds max serialized size of {}'\
                               .format(self.maxitemsize))

        self.itemsq.put(jsonencode(item))
        if self.itemsq.full():
            self.uploader.interrupt()
        return self._nextid.next()

    def flush(self):
        self.flushme = True
        self._waitforq()
        self.flushme = False

    def close(self, block=True):
        self.closed = True
        if block:
            self._waitforq()

    def _waitforq(self):
        self.uploader.interrupt()
        self.itemsq.join()

    def __str__(self):
        return self.url


def _encode_identity(iter):
    data = StringIO()
    for item in iter:
        data.write(item)
        data.write('\n')
    return data.getvalue()


def _encode_gzip(iter):
    data = StringIO()
    with GzipFile(fileobj=data, mode='w') as gzo:
        for item in iter:
            gzo.write(item)
            gzo.write('\n')
    return data.getvalue()
