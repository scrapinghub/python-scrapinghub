from requests.exceptions import HTTPError
from .resourcetype import ResourceType
from .utils import urlpathjoin


class Frontier(ResourceType):

    resource_type = 'hcf'

    batch_size = 100  # max upload batch size supported by hubstorage
    batch_qsize = None  # defaults to twice batch_size if None
    batch_start = 0
    batch_interval = 15.0
    batch_append = False
    batch_content_encoding = 'identity'

    _writers = {}  # dict of writers indexed by (frontier, slot)

    def _get_writer(self, frontier, slot):
        key = (frontier, slot)
        writer = self._writers.get(key)
        if not writer:
            writer = self.client.batchuploader.create_writer(
                url=urlpathjoin(self.url, frontier, 's', slot),
                auth=self.auth,
                size=self.batch_size,
                start=self.batch_start,
                interval=self.batch_interval,
                qsize=self.batch_qsize,
                content_encoding=self.batch_content_encoding
            )
            self._writers[key] = writer
        return writer

    def close(self, block=True):
        for writer in self._writers.values():
            writer.close(block=block)

    def add(self, frontier, slot, fps):
        writer = self._get_writer(frontier, slot)
        for fp in fps:
            writer.write(fp)
        writer.flush()

    def read(self, frontier, slot):
        path = urlpathjoin(frontier, 's', slot, 'q')
        return self.apiget(path)

    def delete(self, frontier, slot, ids):
        path = urlpathjoin(frontier, 's', slot, 'q', 'deleted')
        self.apipost(path, jl=ids)

    def delete_slot(self, frontier, slot):
        path = urlpathjoin(frontier, 's', slot)
        self.apidelete(path)
