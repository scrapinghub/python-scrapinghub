from .resourcetype import ResourceType
from .utils import urlpathjoin


class Collections(ResourceType):

    resource_type = 'collections'

    def get(self, _type, _name, _key=None, **params):
        path = urlpathjoin(_type, _name, _key)
        r = self.apiget(path, params=params)
        return r if _key is None else r.next()

    def set(self, _type, _name, _values):
        path = urlpathjoin(_type, _name)
        return self.apipost(path, jl=_values)

    def delete(self, _type, _name, _keys):
        path = urlpathjoin(_type, _name, 'deleted')
        return self.apipost(path, jl=_keys)

    def new_store(self, colname):
        return Collection('s', colname, self)

    def new_cached_store(self, colname):
        return Collection('cs', colname, self)

    def new_versioned_store(self, colname):
        return Collection('vs', colname, self)

    def new_versioned_cached_store(self, colname):
        return Collection('vcs', colname, self)


class Collection(object):

    def __init__(self, coltype, colname, collections):
        self.coltype = coltype
        self.colname = colname
        self._collections = collections

    def get(self, *args, **kwargs):
        return self._collections.get(self.coltype, self.colname, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._collections.set(self.coltype, self.colname, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._collections.delete(self.coltype, self.colname, *args, **kwargs)
