import logging
from collections import MutableMapping
from .utils import urlpathjoin, xauth
from .serialization import jlencode, jldecode

logger = logging.getLogger('hubstorage.resourcetype')


class ResourceType(object):

    resource_type = None
    key_suffix = None

    def __init__(self, client, key, auth=None):
        self.client = client
        self.key = urlpathjoin(self.resource_type, key, self.key_suffix)
        self.auth = xauth(auth) or client.auth
        self.url = urlpathjoin(client.endpoint, self.key)

    def apirequest(self, _path=None, **kwargs):
        kwargs['url'] = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        kwargs.setdefault('timeout', self.client.connection_timeout)
        if 'jl' in kwargs:
            kwargs['data'] = jlencode(kwargs.pop('jl'))

        r = self.client.session.request(**kwargs)
        if not r.ok:
            logger.debug('%s: %s', r, r.content)
        r.raise_for_status()
        return jldecode(r.iter_lines())

    def apipost(self, _path=None, **kwargs):
        return self.apirequest(_path, method='POST', **kwargs)

    def apiget(self, _path=None, **kwargs):
        return self.apirequest(_path, method='GET', **kwargs)

    def apidelete(self, _path=None, **kwargs):
        return self.apirequest(_path, method='DELETE', **kwargs)


class ItemsResourceType(ResourceType):

    batch_size = 1000
    batch_qsize = None  # defaults to twice batch_size if None
    batch_start = 0
    batch_interval = 15.0
    batch_append = False
    batch_content_encoding = 'identity'

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
                content_encoding=self.batch_content_encoding
            )
        return self._writer

    def _get_itemcount(self):
        return self.stats().get('totals', {}).get('input_values', 0)

    def flush(self):
        if self._writer is not None:
            self._writer.flush()

    def close(self, block=True):
        if self._writer is not None:
            self._writer.close(block=block)

    def write(self, item):
        return self.writer.write(item)

    def list(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def get(self, _key, **params):
        """Return first matching result"""
        for o in self.list(_key, params=params):
            return o

    def stats(self):
        return self.apiget('stats').next()


class MappingResourceType(ResourceType, MutableMapping):

    _cached = None
    ignore_fields = ()

    def __init__(self, *a, **kw):
        self._cached = kw.pop('cached', None)
        self._deleted = set()
        super(MappingResourceType, self).__init__(*a, **kw)

    @property
    def _data(self):
        if self._cached is None:
            r = self.apiget()
            try:
                self._cached = r.next()
            except StopIteration:
                self._cached = {}

        return self._cached

    def expire(self):
        self._cached = None

    def save(self):
        for key in self._deleted:
            self.apidelete(key)
        self._deleted.clear()
        if self._cached:
            if not self.ignore_fields:
                self.apipost(jl=self._data)
            else:
                self.apipost(jl=dict((k, v) for k, v in self._data.iteritems()
                                     if k not in self.ignore_fields))

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        self._deleted.discard(key)

    def __delitem__(self, key):
        del self._data[key]
        self._deleted.add(key)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def liveget(self, key):
        for o in self.apiget(key):
            return o
