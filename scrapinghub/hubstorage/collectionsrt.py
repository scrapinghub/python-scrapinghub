import re

from requests.exceptions import HTTPError

from .resourcetype import DownloadableResource
from .utils import urlpathjoin


COLLECTIONS_MSGPACK_REGEX = re.compile(
    r"""(v?c?s)  # collection type
        /\w+     # collection name
        (
            /?                 # no key
            |                  # OR
            /(?P<key>[^/]+)/?  # item key
        )
        $
    """,
    re.VERBOSE)


class Collections(DownloadableResource):

    resource_type = 'collections'

    def _allows_mpack(self, path=None):
        """Check if request can be served with msgpack data.

        Collection scan and get requests for keys are able to return msgpack data.

        :param path: None, tuple or string

        """
        if not self.client.use_msgpack:
            return False
        path = urlpathjoin(path or '')
        match = COLLECTIONS_MSGPACK_REGEX.match(path)
        # count endpoint doesn't support msgpack
        return bool(match and match.group('key') != 'count')

    def get(self, _type, _name, _key=None, **params):
        try:
            r = self.apiget((_type, _name, _key), params=params)
            return r if _key is None else next(r)
        except HTTPError as exc:
            if exc.response.status_code == 404:
                raise KeyError(_key)
            elif exc.response.status_code == 400:
                raise ValueError(exc.response.text)
            else:
                raise

    def set(self, _type, _name, _values):
        try:
            return self.apipost((_type, _name), is_idempotent=True, jl=_values)
        except HTTPError as exc:
            if exc.response.status_code in (400, 413):
                raise ValueError(exc.response.text)
            else:
                raise

    def delete(self, _type, _name, _keys):
        return self.apipost((_type, _name, 'deleted'), is_idempotent=True, jl=_keys)

    def truncate(self, _name):
        return self.apipost('delete', params={'name': _name})

    def iter_json(self, _type, _name, requests_params=None, **apiparams):
        return DownloadableResource.iter_json(self, (_type, _name),
            requests_params=requests_params, **apiparams)

    def iter_msgpack(self, _type, _name, requests_params=None, **apiparams):
        return DownloadableResource.iter_msgpack(self, (_type, _name),
            requests_params=requests_params, **apiparams)

    def create_writer(self, coltype, colname, **writer_kwargs):
        self._validate_collection(coltype, colname)
        kwargs = dict(writer_kwargs)
        kwargs.setdefault('content_encoding', 'gzip')
        kwargs.setdefault('auth', self.auth)
        url = urlpathjoin(self.url, coltype, colname)
        return self.client.batchuploader.create_writer(url,
            **kwargs)

    def new_collection(self, coltype, colname):
        self._validate_collection(coltype, colname)
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

    def _validate_collection(self, coltype, colname):
        if coltype not in {'s', 'cs', 'vs', 'vcs'}:
            raise ValueError('Invalid collection type: {}'.format(coltype))

        if not re.match(r'^\w+$', colname):
            raise ValueError('Invalid collection name {!r}, only alphanumeric '
                             'characters'.format(colname))


    def _batch(self, method, path, total_param, progress=None, **params):
        total = 0
        getparams = dict(params)
        try:
            while True:
                r = next(self.apirequest(path, method=method, params=getparams))
                total += r[total_param]
                next_start = r.get('nextstart')
                if next_start is None:
                    break
                getparams['start'] = next_start
                if progress:
                    progress(total, next_start)
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

    def create_writer(self, **kwargs):
        """Create a writer for async writing of bulk data

        kwargs are passed to batchuploader.create_writer, but auth and gzip
        content encoding are specified if not provided
        """
        return self._collections.create_writer(self.coltype, self.colname,
            **kwargs)

    def get(self, *args, **kwargs):
        return self._collections.get(self.coltype, self.colname, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._collections.set(self.coltype, self.colname, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._collections.delete(self.coltype, self.colname, *args, **kwargs)

    def truncate(self):
        return self._collections.truncate(self.colname)

    def count(self, *args, **kwargs):
        return self._collections.count(self.coltype, self.colname, *args, **kwargs)

    def iter_json(self, requests_params=None, **apiparams):
        return self._collections.iter_json(self.coltype, self.colname,
            requests_params=requests_params, **apiparams)

    def iter_values(self, requests_params=None, **apiparams):
        return self._collections.iter_values(self.coltype, self.colname,
            requests_params=requests_params, **apiparams)
