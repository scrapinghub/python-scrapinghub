from datetime import datetime
from json import loads, dumps
from .utils import urlpathjoin, xauth


class ResourceType(object):

    resource_type = None

    def __init__(self, client, key, auth=None):
        self.key = urlpathjoin(self.resource_type, key)
        self.client = client
        self.auth = xauth(auth) or client.auth
        self.url = urlpathjoin(client.endpoint, self.key)

    def apirequest(self, _path=None, **kwargs):
        kwargs['url'] = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        if 'jl' in kwargs:
            kwargs['data'] = self._jlencode(kwargs.pop('jl'))

        r = self.client.session.request(**kwargs)
        r.raise_for_status()
        return self._jldecode(r.iter_lines())

    def apipost(self, _path=None, **kwargs):
        return self.apirequest(_path, method='POST', **kwargs)

    def apiget(self, _path=None, **kwargs):
        return self.apirequest(_path, method='GET', **kwargs)

    def apidelete(self, _path=None, **kwargs):
        return self.apirequest(_path, method='DELETE', **kwargs)

    def _jlencode(self, iterable):
        if isinstance(iterable, (dict, str, unicode)):
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

