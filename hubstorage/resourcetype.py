from datetime import datetime
from json import loads, dumps
from .utils import urlpathjoin, xauth
from .serialization import jlencode, jldecode


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
            kwargs['data'] = jlencode(kwargs.pop('jl'))

        r = self.client.session.request(**kwargs)
        r.raise_for_status()
        return jldecode(r.iter_lines())

    def apipost(self, _path=None, **kwargs):
        return self.apirequest(_path, method='POST', **kwargs)

    def apiget(self, _path=None, **kwargs):
        return self.apirequest(_path, method='GET', **kwargs)

    def apidelete(self, _path=None, **kwargs):
        return self.apirequest(_path, method='DELETE', **kwargs)

