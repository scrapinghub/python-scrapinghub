
from .resourcetype import ResourceType
from .utils import urlpathjoin


class Frontier(ResourceType):

    resource_type = 'hcf'

    batch_size = 5000
    batch_qsize = 6000  # defaults to twice batch_size if None
    batch_start = 0
    batch_interval = 60.0
    batch_append = False
    batch_content_encoding = 'identity'

    def __init__(self, *a, **kw):
        self._writers = {}  # dict of writers indexed by (frontier, slot)
        self.newcount = 0
        super(Frontier, self).__init__(*a, **kw)

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
                content_encoding=self.batch_content_encoding,
                callback=self._writer_callback
            )
            self._writers[key] = writer
        return writer

    def _writer_callback(self, response):
        self.newcount += response.json()["newcount"]

    def close(self, block=True):
        for writer in self._writers.values():
            writer.close(block=block)

    def flush(self):
        for writer in self._writers.values():
            writer.flush()

    def add(self, frontier, slot, fps):
        writer = self._get_writer(frontier, slot)
        for fp in fps:
            writer.write(fp)

    def read(self, frontier, slot, mincount=None):
        params = {}
        if mincount is not None:
            params['mincount'] = mincount
        return self.apiget((frontier, 's', slot, 'q'), params=params)

    def delete(self, frontier, slot, ids):
        self.apipost((frontier, 's', slot, 'q/deleted'), jl=ids)

    def delete_slot(self, frontier, slot):
        self.apidelete((frontier, 's', slot))
