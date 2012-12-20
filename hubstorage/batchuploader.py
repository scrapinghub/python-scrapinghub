import time, atexit, logging, warnings, socket
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
    checkpoint_interval = 15
    StopThread = object()

    def __init__(self, client):
        self.client = client
        self.closed = False
        self._batchq = Queue()
        self._writers = deque()
        self._batch_thread = Thread(target=self._batch_worker)
        self._batch_thread.daemon = True
        self._batch_thread.start()
        self._writer_thread = Thread(target=self._writer_worker)
        self._writer_thread.daemon = True
        self._writer_thread.start()
        atexit.register(self._atexit)

    def create_writer(self, url, offset=0, auth=None, batchsize=1000):
        auth = xauth(auth) or self.client.auth
        w = _BatchWriter(url=url,
                         auth=auth,
                         offset=offset,
                         batchsize=batchsize)
        self._writers.append(w)
        return w

    def close(self):
        self.closed = True
        for w in self._writers:
            w.close(block=False)

        self._writer_thread.join()
        self._batch_thread.join()

    def _atexit(self):
        if not self.closed:
            warnings.warn("%r not closed properly, some items may have been lost!" % self)

    def __del__(self):
        if not self.closed:
            self.close()

    def _writer_worker(self):
        while self._writers or not self.closed:
            closed = []
            now = time.time()
            ts = now - self.checkpoint_interval
            for w in self._writers:
                q = w.itemsq
                if q.qsize() >= w.batchsize or w.checkpoint < ts or w.closed:
                    self._writer_checkpoint(w)
                    w.checkpoint = now
                    if w.closed and q.empty():
                        closed.append(w)

            for w in closed:
                self._writers.remove(w)

            time.sleep(1)

        # shutdown uploader thread
        self._batchq.put(self.StopThread)

    def _writer_checkpoint(self, w):
        count = 0
        data = StringIO()
        q = w.itemsq
        while count < w.batchsize:
            try:
                item = q.get_nowait()
                data.write(item + u'\n')
                count += 1
            except Empty:
                break

        if count > 0:
            # Send batch to uploader thread
            self._batchq.put({
                'url': w.url,
                'offset': w.offset,
                'data': data.getvalue(),
                'auth': w.auth,
            })
            # Offset must be increased after sending the batch
            w.offset += count
            for _ in xrange(count):
                q.task_done()

    def _batch_worker(self):
        q = self._batchq
        while True:
            batch = q.get()
            if batch is self.StopThread:
                q.task_done()
                break

            self._batch_tryupload(batch)
            q.task_done()
            time.sleep(1)

    def _batch_tryupload(self, batch):
        # TODO: Implements exponential backoff and a global timeout limit
        while True:
            try:
                self._batch_upload(batch)
                break
            except (socket.error, requests.RequestException) as e:
                if isinstance(e, requests.HTTPError):
                    r = e.response
                    msg = "[HTTP error %d] %s" % (r.status_code, r.text.rstrip())
                else:
                    msg = str(e)
                logger.warning("Failed writing data %s: %s", batch['url'], msg)
                time.sleep(self.retry_wait_time)

    def _batch_upload(self, batch):
        self.client.session.request(
            method='POST',
            url=batch['url'],
            data=batch['data'],
            auth=batch['auth'],
            params={'start': batch['offset']},
        )


class _BatchWriter(object):

    def __init__(self, url, offset, auth, batchsize):
        self.url = url
        self.offset = offset
        self.auth = xauth(auth)
        self.batchsize = batchsize
        self.checkpoint = time.time()
        self.itemsq = Queue()
        self.closed = False

    def write(self, item):
        assert not self.closed, 'attempting writes to a closed writer'
        self.itemsq.put(jsonencode(item))

    def close(self, block=True):
        self.closed = True
        if block:
            self.itemsq.join()
