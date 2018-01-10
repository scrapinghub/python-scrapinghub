import time
import json
import socket
import logging
from collections import MutableMapping

import six
from six.moves import range
import requests.exceptions as rexc

from .utils import urlpathjoin, xauth
from .serialization import jlencode, jldecode, mpdecode


logger = logging.getLogger('hubstorage.resourcetype')
CHUNK_SIZE = 512
STATS_CHUNK_SIZE = 512 * 1024


class ResourceType(object):

    resource_type = None
    key_suffix = None

    def __init__(self, client, key, auth=None):
        self.client = client
        self.key = urlpathjoin(self.resource_type, key, self.key_suffix)
        self.auth = xauth(auth) or client.auth
        self.url = urlpathjoin(client.endpoint, self.key)

    def _allows_mpack(self, path=None):
        """Check if request can be served with msgpack data.

        Currently, items, logs and samples endpoints are able to
        return msgpack data. However, /stats calls can only return JSON data
        for now.

        :param path: None, tuple or string

        """
        if not self.client.use_msgpack:
            return False
        path = urlpathjoin(path or '')
        return (
            self.resource_type in ('items', 'logs', 'samples') and
            not path.rstrip('/').endswith('stats')
        )

    @staticmethod
    def _enforce_msgpack(**kwargs):
        kwargs.setdefault('headers', {})
        kwargs['headers']['Accept'] = 'application/x-msgpack'
        return kwargs

    def _iter_content(self, _path, **kwargs):
        kwargs['url'] = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        return self.client.request(**kwargs).iter_content(CHUNK_SIZE)

    def _iter_lines(self, _path, **kwargs):
        kwargs['url'] = urlpathjoin(self.url, _path)
        kwargs.setdefault('auth', self.auth)
        chunk_size = kwargs.pop('chunk_size', CHUNK_SIZE)
        if 'jl' in kwargs:
            # XXX explicitly encode data to overcome shazow/urllib3#717
            # when dealing with large POST requests with enabled TLS
            kwargs['data'] = jlencode(kwargs.pop('jl')).encode('utf-8')

        r = self.client.request(**kwargs)

        lines = r.iter_lines(chunk_size=chunk_size)
        if six.PY3:
            return (l.decode(r.encoding or 'utf8') for l in lines)
        return lines

    def apirequest(self, _path=None, **kwargs):
        if self._allows_mpack(_path) and kwargs.get('method').upper() == 'GET':
            kwargs = self._enforce_msgpack(**kwargs)
            return mpdecode(self._iter_content(_path=_path, **kwargs))
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

    @staticmethod
    def _add_key_meta(params):
        """Adds meta=_key to ensure a key is returned"""
        meta = params.get('meta', [])
        if '_key' not in meta:
            meta = list(meta)
            meta.append('_key')
            params['meta'] = meta
        return params

    def _add_resume_param(self, lastline, offset, params):
        """Adds a startafter=LASTKEY parameter if there was a lastvalue"""
        if lastline is not None:
            lastvalue = json.loads(lastline)
            params['startafter'] = lastvalue['_key']
            if 'start' in params:
                del params['start']

    def iter_values(self, *args, **kwargs):
        """Reliably iterate through all data as python objects

        calls either iter_json or iter_msgpack, decoding the results
        """
        if self._allows_mpack():
            return mpdecode(self.iter_msgpack(*args, **kwargs))
        return jldecode(self.iter_json(*args, **kwargs))

    def _retry(self, iter_callback, resume=False, _path=None, requests_params=None, **apiparams):
        """Reliable iterate through all data calling iter_callback"""
        self._add_key_meta(apiparams)
        lastexc = None
        chunk = None
        offset = 0
        for attempt in range(self.MAX_RETRIES):
            if resume:
                self._add_resume_param(chunk, offset, apiparams)
            try:
                for chunk in iter_callback(_path=_path, params=apiparams,
                                           **requests_params):
                    yield chunk
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
                         "last error was: %s", self.MAX_RETRIES, url,
                         apiparams, lastexc)

    def iter_msgpack(self, _path=None, requests_params=None, **apiparams):
        """Reliably iterate through all data as msgpack"""
        requests_params = dict(requests_params or {})
        requests_params.setdefault('method', 'GET')
        requests_params.setdefault('stream', True)
        requests_params = self._enforce_msgpack(**requests_params)
        for chunk in self._retry(self._iter_content, False, _path,
                                 requests_params, **apiparams):
            yield chunk

    def iter_json(self, _path=None, requests_params=None, **apiparams):
        """Reliably iterate through all data as json strings"""
        requests_params = dict(requests_params or {})
        requests_params.setdefault('method', 'GET')
        requests_params.setdefault('stream', True)
        for line in self._retry(self._iter_lines, True, _path, requests_params,
                                **apiparams):
            yield line


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
        return next(self.apiget('stats', chunk_size=STATS_CHUNK_SIZE))


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
                self._cached = next(r)
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
                self.apipost(jl={k: v for k, v in six.iteritems(self._data)
                                 if k not in self.ignore_fields},
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
