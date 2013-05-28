from requests.exceptions import HTTPError
from .resourcetype import ResourceType
from .utils import urlpathjoin, chunks


class Frontier(ResourceType):

    resource_type = 'hcf'
    max_chunk = 100  # hardcoded in hubstorage

    def add(self, frontier, slot, fps):
        path = urlpathjoin(frontier, 's', slot)
        # the API does not support chunks greater than 100
        for chunk in chunks(fps, self.max_chunk):
            self.apipost(path, jl=chunk)

    def read(self, frontier, slot):
        path = urlpathjoin(frontier, 's', slot, 'q')
        return self.apiget(path)

    def delete(self, frontier, slot, ids):
        path = urlpathjoin(frontier, 's', slot, 'q', 'deleted')
        self.apipost(path, jl=ids)

    def delete_slot(self, frontier, slot):
        path = urlpathjoin(frontier, 's', slot)
        self.apidelete(path)
