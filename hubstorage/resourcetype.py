import logging, time, json, socket
from collections import MutableMapping
import requests.exceptions as rexc
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

    def _iter_lines(self, _path, **kwargs):
        kwargs['url'] = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        if 'jl' in kwargs:
            kwargs['data'] = jlencode(kwargs.pop('jl'))

        r = self.client.request(**kwargs)

        return r.iter_lines()

    def apirequest(self, _path=None, **kwargs):
        return jldecode(self._iter_lines(_path, **kwargs))

    def apipost(self, _path=None, **kwargs):
        return self.apirequest(_path, method='POST', **kwargs)

    def apiget(self, _path=None, **kwargs):
        kwargs.setdefault('is_idempotent', True)
        return self.apirequest(_path, method='GET', **kwargs)

    def apidelete(self, _path=None, **kwargs):
        kwargs.setdefault('is_idempotent', True)
        return self.apirequest(_path, method='DELETE', **kwargs)


class DownloadableResource(ResourceType):
    MAX_RETRIES = 180
    RETRY_INTERVAL = 60

    def _add_resume_param(self, lastline, offset, params):
        """Adds a startafter=LASTKEY parameter if there was
        a lastvalue. It also adds meta=_key to ensure a key is returned
        """
        meta = params.get('meta', [])
        if '_key' not in meta:
            meta = list(meta)
            meta.append('_key')
            params['meta'] = meta
        if lastline is not None:
            lastvalue = json.loads(lastline)
            params['startafter'] = lastvalue['_key']
            if 'start' in params:
                del params['start']

    def iter_values(self, *args, **kwargs):
        """Reliably iterate through all data as python objects

        calls iter_json, decoding the results
        """
        return jldecode(self.iter_json(*args, **kwargs))

    def iter_json(self, _path=None, requests_params=None, **apiparams):
        """Reliably iterate through all data as json strings"""
        requests_params = dict(requests_params or {})
        requests_params.setdefault('method', 'GET')
        requests_params.setdefault('stream', True)
        lastexc = None
        line = None
        offset = 0
        for attempt in xrange(self.MAX_RETRIES):
            self._add_resume_param(line, offset, apiparams)
            try:
                for line in self._iter_lines(_path=_path, params=apiparams,
                        **requests_params):
                    yield line
                    offset += 1
                break
            except (ValueError, socket.error, rexc.RequestException) as exc:
                # catch requests exceptions other than HTTPError
                if isinstance(exc, rexc.HTTPError):
                    raise
                lastexc = exc
                url = urlpathjoin(self.url, _path)
                msg = "Retrying read of %s in %ds: attempt=%d/%d error=%s"
                args = url, self.RETRY_INTERVAL, attempt, self.MAX_RETRIES, exc
                logger.debug(msg, *args)
                time.sleep(self.RETRY_INTERVAL)
        else:
            url = urlpathjoin(self.url, _path)
            logger.error("Failed %d times reading items from %s, params %s, "
                "last error was: %s", self.MAX_RETRIES, url, apiparams, lastexc)


class ItemsResourceType(ResourceType):

    batch_size = 1000
    batch_qsize = None  # defaults to twice batch_size if None
    batch_start = 0
    batch_interval = 15.0
    batch_content_encoding = 'identity'

    # batch writer reference in case of used
    _writer = None

    # TODO override _add_resume_param - can avoid requestomg _key by
    # deriving from project, spider, job and offset

    def batch_write_start(self):
        """Override to set a start parameter when commencing writing"""
        return 0

    @property
    def writer(self):
        if self._writer is None:
            self._writer = self.client.batchuploader.create_writer(
                url=self.url,
                auth=self.auth,
                size=self.batch_size,
                start=self.batch_write_start(),
                interval=self.batch_interval,
                qsize=self.batch_qsize,
                content_encoding=self.batch_content_encoding
            )
        return self._writer

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

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._data))

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
                self.apipost(jl=self._data, is_idempotent=True)
            else:
                self.apipost(jl=dict((k, v) for k, v in self._data.iteritems()
                                     if k not in self.ignore_fields),
                             is_idempotent=True)

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
