from __future__ import absolute_import

import os
import json
import logging
import binascii
from codecs import decode

import six

from ..hubstorage.resourcetype import DownloadableResource
from ..hubstorage.resourcetype import ItemsResourceType
from ..hubstorage.collectionsrt import Collections

from .exceptions import wrap_value_too_large


class LogLevel(object):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    SILENT = CRITICAL + 1


class JobKey(object):

    def __init__(self, project_id, spider_id, job_id):
        self.project_id = project_id
        self.spider_id = spider_id
        self.job_id = job_id

    def __str__(self):
        return '{}/{}/{}'.format(self.project_id, self.spider_id, self.job_id)


def parse_project_id(project_id):
    try:
        int(project_id)
    except ValueError:
        raise ValueError("Project id should be convertible to integer")
    return str(project_id)


def parse_job_key(job_key):
    if isinstance(job_key, tuple):
        parts = job_key
    elif isinstance(job_key, six.string_types):
        parts = job_key.split('/')
    else:
        raise ValueError("Job key should be a string or a tuple")
    if len(parts) != 3:
        raise ValueError(
            "Job key should consist of project_id/spider_id/job_id")
    try:
        map(int, parts)
    except ValueError:
        raise ValueError("Job key parts should be integers")
    return JobKey(*map(str, parts))


def get_tags_for_update(**kwargs):
    """Helper to check tags changes"""
    params = {}
    for k, v in kwargs.items():
        if not v:
            continue
        if not isinstance(v, list):
            raise ValueError("Add/remove field value must be a list")
        params[k] = v
    return params


class _Proxy(object):
    """A helper to create a class instance and proxy its methods to origin.

    The internal proxy class is useful to link class attributes from its
    origin depending on the origin base class as a part of init logic:

    - :class:`ItemsResourceType` provides items-based attributes to access
        items in an arbitrary collection with get/write/flush/close/stats/
        iter methods.

    - :class:`DownloadableResource` provides download-based attributes to
        iter through collection with or without msgpack support.
    """

    def __init__(self, cls, client, key):
        self.key = key
        self._client = client
        self._origin = cls(client._hsclient, key)

        if issubclass(cls, ItemsResourceType):
            self._proxy_methods(['get', 'write', 'flush', 'close',
                                 'stats', ('iter', 'list')])
            # redefine write method to wrap hubstorage.ValueTooLarge error
            origin_method = getattr(self, 'write')
            setattr(self, 'write', wrap_value_too_large(origin_method))

        # DType iter_values() has more priority than IType list()
        # plus Collections interface doesn't need the iter methods
        if issubclass(cls, DownloadableResource) and cls is not Collections:
            methods = [('iter', 'iter_values'),
                       ('iter_raw_msgpack', 'iter_msgpack'),
                       ('iter_raw_json', 'iter_json')]
            self._proxy_methods(methods)
            self._wrap_iter_methods([method[0] for method in methods])

    def _proxy_methods(self, methods):
        """A little helper for cleaner interface."""
        proxy_methods(self._origin, self, methods)

    def _wrap_iter_methods(self, methods):
        """Modify kwargs for all passed self.iter* methods."""
        for method in methods:
            wrapped = wrap_kwargs(getattr(self, method),
                                  self._modify_iter_params)
            setattr(self, method, wrapped)

    def _modify_iter_params(self, params):
        """A helper to modify iter() params on-the-fly.

        The method is internal and should be redefined in subclasses.

        :param params: a dictionary with input parameters.
        :return: an updated dictionary with parameters.
        :rtype: dict
        """
        return format_iter_filters(params)

    def list(self, *args, **kwargs):
        """Convenient shortcut to list iter results.

        Please note that list() method can use a lot of memory and for a large
        amount of elements it's recommended to iterate through it via iter()
        method (all params and available filters are same for both methods).
        """
        return list(self.iter(*args, **kwargs))


class _MappingProxy(_Proxy):
    """A helper class to support basic get/set interface for dict-like
    collections of elements.
    """

    def get(self, key):
        """Get element value by key.

        :param key: a string key
        """
        return next(self._origin.apiget(key))

    def set(self, key, value):
        """Set element value.

        :param key: a string key
        :param value: new value to set for the key
        """
        self._origin.apipost(key, data=json.dumps(value), is_idempotent=True)

    def update(self, values):
        """Update multiple elements at once.

        The method provides convenient interface for partial updates.

        :param values: a dictionary with key/values to update.
        """
        if not isinstance(values, dict):
            raise TypeError("values should be a dict")
        data = next(self._origin.apiget())
        data.update(values)
        self._origin.apipost(jl={k: v for k, v in six.iteritems(data)
                                 if k not in self._origin.ignore_fields},
                             is_idempotent=True)

    def delete(self, key):
        """Delete element by key.

        :param key: a string key
        """
        self._origin.apidelete(key)

    def iter(self):
        """Iterate through key/value pairs.

        :return: an iterator over key/value pairs.
        :rtype: collections.Iterable
        """
        return six.iteritems(next(self._origin.apiget()))


def wrap_kwargs(fn, kwargs_fn):
    """Tiny wrapper to prepare modified version of function kwargs"""
    def wrapped(*args, **kwargs):
        kwargs = kwargs_fn(kwargs)
        return fn(*args, **kwargs)
    return wrapped


def proxy_methods(origin, successor, methods):
    """A helper to proxy methods from origin to successor.

    Accepts a list with strings and tuples:

    - each string defines:
        a successor method name to proxy 1:1 with origin method
    - each tuple should consist of 2 strings:
        a successor method name and an origin method name
    """
    for method in methods:
        if isinstance(method, tuple):
            successor_name, origin_name = method
        else:
            successor_name, origin_name = method, method
        if not hasattr(successor, successor_name):
            setattr(successor, successor_name, getattr(origin, origin_name))


def format_iter_filters(params):
    """Format iter() filter param on-the-fly.

    Support passing multiple filters at once as a list with tuples.
    """
    filters = params.get('filter')
    if filters and isinstance(filters, list):
        filter_data = []
        for elem in params.pop('filter'):
            if isinstance(elem, six.string_types):
                filter_data.append(elem)
            elif isinstance(elem, (list, tuple)):
                filter_data.append(json.dumps(elem))
            else:
                raise ValueError(
                    "Filter condition must be string, tuple or list")
        if filter_data:
            params['filter'] = filter_data
    return params


def update_kwargs(kwargs, **params):
    kwargs.update({k: json.dumps(v) if isinstance(v, dict) else v
                   for k, v in params.items() if v is not None})


def parse_auth(auth):
    """Parse authentification token.

    >>> os.environ['SH_APIKEY'] = 'apikey'
    >>> parse_auth(None)
    ('apikey', '')
    >>> parse_auth(('user', 'pass'))
    ('user', 'pass')
    >>> parse_auth('user:pass')
    ('user', 'pass')
    >>> parse_auth('c3a3c298c2b8c3a6c291c284c3a9')
    ('c3a3c298c2b8c3a6c291c284c3a9', '')
    >>> parse_auth('312f322f333a736f6d652e6a77742e746f6b656e')
    ('1/2/3', 'some.jwt.token')
    """
    if auth is None:
        apikey = os.environ.get('SH_APIKEY')
        if apikey is None:
            raise RuntimeError("No API key provided and SH_APIKEY "
                               "environment variable not set")
        return (apikey, '')

    if isinstance(auth, tuple):
        all_strings = all(isinstance(k, six.string_types) for k in auth)
        if len(auth) != 2 or not all_strings:
            raise ValueError("Wrong authentication credentials")
        return auth

    if not isinstance(auth, six.string_types):
        raise ValueError("Wrong authentication credentials")

    jwt_auth = _search_for_jwt_credentials(auth)
    if jwt_auth:
        return jwt_auth

    login, _, password = auth.partition(':')
    return (login, password)


def _search_for_jwt_credentials(auth):
    try:
        decoded_auth = decode(auth, 'hex_codec')
    except (binascii.Error, TypeError):
        return
    try:
        if not isinstance(decoded_auth, six.string_types):
            decoded_auth = decoded_auth.decode('ascii')
        login, _, password = decoded_auth.partition(':')
        if password and parse_job_key(login):
            return (login, password)
    except (UnicodeDecodeError, ValueError):
        pass
