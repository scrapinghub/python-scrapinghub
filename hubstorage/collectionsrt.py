from requests.exceptions import HTTPError
from .resourcetype import ResourceType


class Collections(ResourceType):

    resource_type = 'collections'

    def get(self, _type, _name, _key=None, **params):
        try:
            r = self.apiget((_type, _name, _key), params=params)
            return r if _key is None else r.next()
        except HTTPError as exc:
            if exc.response.status_code == 404:
                raise KeyError(_key)
            elif exc.response.status_code == 400:
                raise ValueError(exc.response.text)
            else:
                raise

    def set(self, _type, _name, _values):
        try:
            return self.apipost((_type, _name), jl=_values)
        except HTTPError as exc:
            if exc.response.status_code in (400, 413):
                raise ValueError(exc.response.text)
            else:
                raise

    def delete(self, _type, _name, _keys):
        return self.apipost((_type, _name, 'deleted'), jl=_keys)

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

    def count(self, _type, _name, **params):
        return self._batch('GET', (_type, _name, 'count'), 'count', **params)

    def delete_all(self, _type, _name, **params):
        return self._batch('DELETE', (_type, _name), 'deleted', **params)

    def _batch(self, method, path, total_param, progress=None, **params):
        total = 0
        getparams = dict(params)
        try:
            while True:
                r = self.apirequest(path, method=method,
                    params=getparams).next()
                total += r[total_param]
                next = r.get('nextstart')
                if next is None:
                    break
                getparams['start'] = next
                if progress:
                    progress(total, next)
            return total
        except HTTPError as exc:
            if exc.response.status_code == 400:
                raise ValueError(exc.response.text)
            else:
                raise


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

    def delete_all(self, *args, **kwargs):
        return self._collections.delete_all(self.coltype, self.colname, *args, **kwargs)

    def count(self, *args, **kwargs):
        return self._collections.count(self.coltype, self.colname, *args, **kwargs)
