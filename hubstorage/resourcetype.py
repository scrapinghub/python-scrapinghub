from datetime import datetime
from json import loads, dumps
from .utils import urlpathjoin


class ResourceType(object):

    resource_type = None

    def __init__(self, key, client, auth, resource_type=None):
        self.key = key
        self.client = client
        self.auth = auth
        if resource_type is not None:
            self.resource_type = resource_type

        self.url = urlpathjoin(client.endpoint, self.resource_type, key)

    def _rawpost(self, _path=None, **kwargs):
        url = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        return self.client.conn.post(url, **kwargs)

    def _rawget(self, _path=None, **kwargs):
        url = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        return self.client.conn.get(url, **kwargs)

    def apipost(self, _path=None, **kwargs):
        """POST an iterable of docs and returns an interable of results"""
        if 'jl' in kwargs:
            kwargs['data'] = self._jlencode(kwargs.pop('jl'))

        r = self._rawpost(_path, **kwargs)
        r.raise_for_status()
        return self._jldecode(r.iter_lines())

    def apiget(self, _path=None, **kwargs):
        """GET to endpoint and return an iterable of results"""
        r = self._rawget(_path, **kwargs)
        r.raise_for_status()
        return self._jldecode(r.iter_lines())

    def _jlencode(self, iterable):
        if isinstance(iterable, dict):
            iterable = [iterable]
        return u'\n'.join(dumps(o, default=self._jldefault) for o in iterable)

    def _jldecode(self, lineiterable):
        for line in lineiterable:
            yield loads(line)

    EPOCH = datetime.utcfromtimestamp(0)
    def _jldefault(self, o):
        if isinstance(o, datetime):
            delta = o - self.EPOCH
            differenceTotalMillis = (delta.microseconds +
                    (delta.seconds + delta.days*24*3600) * 1e6) / 1000
            return int(differenceTotalMillis)
        else:
            return str(o)

