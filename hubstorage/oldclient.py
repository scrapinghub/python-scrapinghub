"""HubStorage client library"""

import json, requests
from urlparse import urljoin
from .itemwriter import ItemWriter


class Client(object):

    def __init__(self, auth, url="http://localhost:8002"):
        p = auth.partition(':')
        self.auth = p[0], p[2]
        self.url = url

    def open_item_writer(self, path):
        return ItemWriter(self, self._items_url(path))

    def iter_items(self, path, method='GET', data=None):
        return (json.loads(x) for x in self.iter_json_items(path, method, data))

    def iter_json_items(self, path, method='GET', data=None):
        r = requests.request(method, self._items_url(path), prefetch=False,
            auth=self.auth, data=data)
        r.raise_for_status()
        return r.iter_lines()

    def _items_url(self, path):
        return urljoin(self.url, path)

