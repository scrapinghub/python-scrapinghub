from requests.exceptions import HTTPError
from .resourcetype import ResourceType
from .utils import urlpathjoin


class Collections(ResourceType):

    resource_type = 'collections'

    def get(self, _type, _name, _key=None, **params):
        path = urlpathjoin(_type, _name, _key)
        try:
            r = self.apiget(path, params=params)
            return r if _key is None else r.next()
        except HTTPError as exc:
            if exc.response.status_code == 404:
                raise KeyError(_key)
            elif exc.response.status_code == 400:
                raise ValueError(exc.response.text)
            else:
                raise

    def set(self, _type, _name, _values):
        path = urlpathjoin(_type, _name)
        try:
            return self.apipost(path, jl=_values)
        except HTTPError as exc:
            if exc.response.status_code in (400, 413):
                raise ValueError(exc.response.text)
            else:
                raise

    def delete(self, _type, _name, _keys):
        path = urlpathjoin(_type, _name, 'deleted')
        return self.apipost(path, jl=_keys)

    def new_collection(self, coltype, colname):
        return Collection(coltype, colname, self)

    def new_store(self, colname):
        return self.new_collection('s', colname)

    def new_cached_store(self, colname):
        return self.new_collection('cs', colname)

    def new_versioned_store(self, colname):
        return self.new_collection('vs', colname)

    def new_versioned_cached_store(self, colname):
        return self.new_collection('vcs', colname)


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
