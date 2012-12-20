import time, atexit, logging, warnings, socket
from gzip import GzipFile
import requests
from requests.compat import StringIO
from collections import deque
from Queue import Queue, Empty
from threading import Thread
from .utils import xauth
from .serialization import jsonencode

logger = logging.getLogger('hubstorage.batchuploader')


class BatchUploader(object):

    retry_wait_time = 5.0

    def __init__(self, client):
        self.client = client
        self.closed = False
        self._writers = deque()
        self._thread = Thread(target=self._worker)
        self._thread.daemon = True
        self._thread.start()
        atexit.register(self._atexit)

    def create_writer(self, url, start=0, auth=None, size=1000, interval=15, qsize=None):
        auth = xauth(auth) or self.client.auth
        w = _BatchWriter(url=url,
                         auth=auth,
                         size=size,
                         start=start,
                         interval=interval,
                         qsize=qsize)
        self._writers.append(w)
        return w

    def close(self):
        self.closed = True
        for w in self._writers:
            w.close(block=False)
        self._thread.join()

    def _atexit(self):
        if not self.closed:
            warnings.warn("%r not closed properly, some items may have been lost!" % self)

    def __del__(self):
        if not self.closed:
            self.close()

    def _worker(self):
        while self._writers or not self.closed:
            closed = []
            for w in self._writers:
                q = w.itemsq
                now = time.time()
                ts = now - w.interval
                if q.qsize() >= w.size or w.checkpoint < ts or w.closed:
                    self._checkpoint(w)
                    w.checkpoint = now
                    if w.closed and q.empty():
                        closed.append(w)

            for w in closed:
                self._writers.remove(w)

            time.sleep(1)

    def _checkpoint(self, w):
        count = 0
        q = w.itemsq
        data = StringIO()
        with GzipFile(fileobj=data, mode='w') as gzo:
            while count < w.size:
                try:
                    item = q.get_nowait()
                    gzo.write(item + u'\n')
                    count += 1
                except Empty:
                    break

        if count > 0:
            # Upload batch and retry in case of failures
            self._tryupload({'url': w.url,
                             'offset': w.offset,
                             'data': data.getvalue(),
                             'auth': w.auth})
            # Offset must be increased after sending the batch
            w.offset += count
            for _ in xrange(count):
                q.task_done()

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
        self.client.session.request(method='POST',
                                    url=batch['url'],
                                    data=batch['data'],
                                    auth=batch['auth'],
                                    params={'start': batch['offset']},
                                    headers={'content-encoding': 'gzip'})


class _BatchWriter(object):

    def __init__(self, url, start, auth, size, interval, qsize):
        self.url = url
        self.offset = start
        self.auth = auth
        self.size = size
        self.interval = interval
        self.checkpoint = time.time()
        self.itemsq = Queue(size * 2 if qsize is None else qsize)
        self.closed = False

    def write(self, item):
        assert not self.closed, 'attempting writes to a closed writer'
        self.itemsq.put(jsonencode(item))

    def flush(self):
        self.itemsq.join()

    def close(self, block=True):
        self.closed = True
        if block:
            self.itemsq.join()
