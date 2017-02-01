import time
import socket
import random
import logging
import warnings
import six
from six.moves import range
from six.moves.queue import Queue
from io import BytesIO
from gzip import GzipFile
from itertools import count
import requests
from collections import deque
from threading import Thread, Event
from .utils import xauth, iterqueue, sizeof_fmt
from .serialization import jsonencode

logger = logging.getLogger('hubstorage.batchuploader')


class BatchUploader(object):

    # Wait time between all batches status checks
    worker_loop_delay = 1.0
    # Max number of retry attempts before giving up.
    worker_max_retries = 200
    # The delay increases exponentially with the number of attempts but is
    # bounded with these values on both sides.
    worker_min_interval = 30
    worker_max_interval = 600
    # Each delay is also randomized by multiplying by random(0.5, 1.5), so the
    # total delay using the current parameters is an almost gaussian random
    # number with the following characteristics (see Irwin-Hall distribution):
    #
    # - average = 30hrs
    # - minimum = 15hrs
    # - maximum = 45hrs
    # - standard deviation = approx. 40m, which means that 95% of the time the
    #   total delay will be within 2*std = 1h20m of the average.

    def __init__(self, client):
        self.client = client
        self.closed = False
        self._wait_event = Event()
        self._writers = deque()
        self._thread = Thread(target=self._worker)
        self._thread.daemon = True
        self._thread.start()

    def create_writer(self, url, start=0, auth=None, size=1000, interval=15,
                      qsize=None, content_encoding='identity',
                      maxitemsize=1024 ** 2, callback=None):
        # callback shouldn't try to inject more items in the queue
        # otherwise it can lead to deadlock on _checkpoint step
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
                         uploader=self,
                         callback=callback)
        self._writers.append(w)
        return w

    def close(self, timeout=None):
        self.closed = True
        self.interrupt()
        self._thread.join(timeout)

    def interrupt(self):
        self._wait_event.set()

    def __del__(self):
        if not self.closed:
            warnings.warn("%r not closed properly, some items may have been "
                          "lost!: %r" % (self.__class__.__name__, self._writers))

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
            if (next(ctr) % len(self._writers) == 0) and not self.closed:
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
            response = self._tryupload({
                'url': w.url,
                'offset': w.offset,
                'data': data,
                'auth': w.auth,
                'content-encoding': w.content_encoding,
            })
            w.offset += qiter.count
            if w.callback is not None:
                try:
                    w.callback(response)
                except Exception:
                    logger.exception("Callback for %s failed", w.url)
            for _ in range(qiter.count):
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
        """Retry uploads in case of server failures

        Use polinomial backoff with 10 minutes maximum interval that accounts
        for ~30 hours of total retry time.

        >>> sum(min(x**2, 600) for x in range(200)) / 3600
        30
        """
        url = batch['url']
        offset = batch['offset']
        for retryn in range(self.worker_max_retries):
            emsg = ''
            try:
                r = self._upload(batch)
                r.raise_for_status()
                if not (200 <= r.status_code < 300):
                    logger.warning('Discarding write to url=%s offset=%s: '
                                   '[HTTP error %s] %s\n%s', url, offset,
                                   r.status_code, r.reason, r.text.rstrip())
                return r
            except (socket.error, requests.RequestException) as e:
                if isinstance(e, requests.HTTPError):
                    emsg = "[HTTP error {0}] {1}".format(e.response.status_code,
                                                         e.response.text.rstrip())
                else:
                    emsg = str(e)
                logger.info("Retrying url=%s offset=%s: %s", url, offset, emsg)
            except Exception:
                logger.exception('Non retryable failure on url=%s offset=%s',
                                 url, offset)
                break

            backoff = min(max(retryn ** 2, self.worker_min_interval),
                          self.worker_max_interval)
            time.sleep(backoff * (0.5 + random.random()))

    def _upload(self, batch):
        params = {'start': batch['offset']}
        headers = {'content-encoding': batch['content-encoding']}
        return self.client.session.request(
            method='POST',
            url=batch['url'],
            data=batch['data'],
            auth=batch['auth'],
            timeout=self.client.connection_timeout,
            params=params,
            headers=headers,
        )

class ValueTooLarge(ValueError):
    """Raised when a serialized item is greater than 1MB"""


class _BatchWriter(object):
    #: Truncate overly big items to that many bytes for the error message.
    ERRMSG_DATA_TRUNCATION_LEN = 1024

    def __init__(self, url, start, auth, size, interval, qsize,
                 maxitemsize, content_encoding, uploader, callback=None):
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
        self.callback = callback

    def write(self, item):
        assert not self.closed, 'attempting writes to a closed writer'
        data = jsonencode(item)
        if len(data) > self.maxitemsize:
            truncated_data = data[:self.ERRMSG_DATA_TRUNCATION_LEN] + "..."
            raise ValueTooLarge(
                'Value exceeds max encoded size of {}: {!r}'
                .format(sizeof_fmt(self.maxitemsize), truncated_data))

        self.itemsq.put(data)
        if self.itemsq.full():
            self.uploader.interrupt()
        return next(self._nextid)

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


def _encode_identity(iterable):
    data = BytesIO()
    for item in iterable:
        if isinstance(item, six.text_type):
            item = item.encode('utf8')
        data.write(item)
        data.write(b'\n')
    return data.getvalue()


def _encode_gzip(iterable):
    data = BytesIO()
    with GzipFile(fileobj=data, mode='w') as gzo:
        for item in iterable:
            if isinstance(item, six.text_type):
                item = item.encode('utf8')
            gzo.write(item)
            gzo.write(b'\n')
    return data.getvalue()
