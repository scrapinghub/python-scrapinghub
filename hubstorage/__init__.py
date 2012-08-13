"""HubStorage client library"""

import atexit, warnings
from urlparse import urljoin
from threading import Thread, Event
from Queue import Queue, Empty
import requests, json, time, logging

log = logging.getLogger('hubstorage')

class Client(object):

    def __init__(self, apikey, url="http://localhost:8002"):
        self.apikey = apikey
        self.url = url

    def open_item_writer(self, path):
        return ItemWriter(self, self._items_url(path))

    def iter_items(self, path):
        return (json.loads(x) for x in self.iter_json_items(path))

    def iter_json_items(self, path):
        r = requests.get(self._items_url(path), auth=(self.apikey, ''))
        r.raise_for_status()
        return r.iter_lines()

    def _items_url(self, path):
        return urljoin(self.url, path)

StopThread = object()

class ItemWriter(object):

    chunk_size = 1000
    retry_wait_time = 5.0

    def __init__(self, client, url):
        self.client = client
        self.url = url
        self.queue = Queue(self.chunk_size)
        self.thread = Thread(target=self._worker)
        self.thread.daemon = True
        self.thread.start()
        self.closed = False
        atexit.register(self._atexit)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        if not self.closed:
            self.close()

    def __repr__(self):
        return "ItemWriter(%r)" % self.url

    def write_item(self, item):
        jsonitem = json.dumps(item)
        self.write_json_item(jsonitem)

    def write_json_item(self, jsonitem):
        if self.closed:
            raise RuntimeError("ItemWriter already closed")
        self.queue.put(jsonitem)

    def close(self):
        self.closed = True
        self.queue.put(StopThread)
        self.thread.join()

    def _worker(self):
        offset = 0
        closing = False
        while not closing:
            item = self.queue.get()
            if item is StopThread:
                break
            items = [item]
            try:
                for _ in xrange(self.chunk_size-1):
                    try:
                        item = self.queue.get_nowait()
                        if item is StopThread:
                            closing = True
                            break
                        items.append(item)
                    except Empty:
                        break
                while True:
                    try:
                        self._upload_items(items, offset)
                        break
                    except requests.RequestException, e:
                        log.warning("Failed uploading items to %s: %s", self.url, str(e))
                        time.sleep(self.retry_wait_time)
            finally:
                for _ in items:
                    self.queue.task_done()
                offset += len(items)

    def _upload_items(self, items, offset):
        data = "\n".join(items)
        headers = {'content-range': 'items %d-/*' % offset}
        r = requests.post(self.url, data=data, headers=headers, prefetch=True,
            auth=(self.client.apikey, ''))
        r.raise_for_status()

    def _atexit(self):
        if not self.closed:
            warnings.warn("%r not closed properly, some items may have been lost!" % self)
