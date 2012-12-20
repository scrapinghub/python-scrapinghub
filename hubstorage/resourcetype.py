from .utils import urlpathjoin, xauth
from .serialization import jlencode, jldecode


class ResourceType(object):

    resource_type = None

    def __init__(self, client, key, auth=None):
        self.client = client
        self.key = urlpathjoin(self.resource_type, key)
        self.auth = xauth(auth) or client.auth
        self.url = urlpathjoin(client.endpoint, self.key)
        self._writer = None

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

    def get_stats(self):
        return self.apiget('stats').next()


class ItemsResourceType(ResourceType):

    batch_size = 1000
    batch_qsize = None  # defaults to twice batch_size if None
    batch_start = 0
    batch_interval = 15.0
    batch_append = False

    # batch writer reference in case of used
    _writer = None

    @property
    def writer(self):
        if self._writer is None:
            start = self._get_itemcount() if self.batch_append else self.batch_start
            self._writer = self.client.batchuploader.create_writer(
                url=self.url,
                auth=self.auth,
                size=self.batch_size,
                start=start,
                interval=self.batch_interval,
                qsize=self.batch_qsize,
            )
        return self._writer

    def _get_itemcount(self):
        return self.get_stats().get('totals', {}).get('input_values', 0)

    def flush(self):
        if self._writer is not None:
            self._writer.flush()

    def get(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def write(self, item):
        self.writer.write(item)
