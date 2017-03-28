from __future__ import absolute_import

import six
import json

from ..hubstorage.resourcetype import DownloadableResource
from ..hubstorage.resourcetype import ItemsResourceType
from ..hubstorage.collectionsrt import Collections

from .exceptions import wrap_value_too_large


class _Proxy(object):
    """A helper to create a class instance and proxy its methods to origin.

    The internal proxy class is useful to link class attributes from its
    origin depending on the origin base class as a part of init logic:

    - :class:`~scrapinghub.hubstorage.resourcetype.ItemsResourceType` provides
        items-based attributes to access items in an arbitrary collection with
        get/write/flush/close/stats/iter methods.

    - :class:`~scrapinghub.hubstorage.resourcetype.DownloadableResource` provides
        download-based attributes to iter through collection with or without
        msgpack support.
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
        :rtype: :class:`dict`
        """
        return format_iter_filters(params)

    def list(self, *args, **kwargs):
        """Convenient shortcut to list iter results.

        Please note that :meth:`list` method can use a lot of memory and for a
        large amount of elements it's recommended to iterate through it via
        :meth:`iter` method (all params and available filters are same for both
        methods).
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
        :rtype: :class:`collections.Iterable`
        """
        return six.iteritems(next(self._origin.apiget()))


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


def wrap_kwargs(fn, kwargs_fn):
    """Tiny wrapper to prepare modified version of function kwargs"""
    def wrapped(*args, **kwargs):
        kwargs = kwargs_fn(kwargs)
        return fn(*args, **kwargs)
    return wrapped
