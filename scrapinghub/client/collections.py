from __future__ import absolute_import
import collections

from six import string_types

from ..hubstorage.collectionsrt import Collection as _Collection

from .utils import _Proxy
from .utils import format_iter_filters
from .utils import proxy_methods
from .utils import wrap_kwargs


class Collections(_Proxy):
    """Access to project collections.

    Not a public constructor: use :class:`Project` instance to get a
    :class:`Collections` instance. See :attr:`Project.collections` attribute.

    Usage::

        >>> collections = project.collections
        >>> collections.list()
        [{'name': 'Pages', 'type': 's'}]
        >>> foo_store = collections.get_store('foo_store')
    """

    def get(self, coltype, colname):
        """Base method to get a collection with a given type and name."""
        self._origin._validate_collection(coltype, colname)
        return Collection(self._client, self, coltype, colname)

    def get_store(self, colname):
        return self.get('s', colname)

    def get_cached_store(self, colname):
        return self.get('cs', colname)

    def get_versioned_store(self, colname):
        return self.get('vs', colname)

    def get_versioned_cached_store(self, colname):
        return self.get('vcs', colname)

    def iter(self):
        """Iterate through collections of a project."""
        return self._origin.apiget('list')

    def list(self):
        """List collections of a project."""
        return list(self.iter())


class Collection(object):
    """Representation of a project collection object.

    Not a public constructor: use :class:`Collections` instance to get a
    :class:`Collection` instance. See :meth:`Collections.get_store` and
    similar methods.  # noqa

    Usage:

    - add a new item to collection::

        >>> foo_store.set({'_key': '002d050ee3ff6192dcbecc4e4b4457d7',
                           'value': '1447221694537'})

    - count items in collection::

        >>> foo_store.count()
        1

    - get an item from collection::

        >>> foo_store.get('002d050ee3ff6192dcbecc4e4b4457d7')
        {'value': '1447221694537'}

    - get all items from collection::

        >>> foo_store.iter()
        <generator object jldecode at 0x1049eef10>

    - iterate iterate over _key & value pair::

        >>> for elem in foo_store.iter(count=1)):
        >>> ... print(elem)
        [{'_key': '002d050ee3ff6192dcbecc4e4b4457d7',
            'value': '1447221694537'}]

    - filter by multiple keys, only values for keys that exist will be returned::

        >>> foo_store.list(key=['002d050ee3ff6192dcbecc4e4b4457d7', 'blah'])
        [{'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'}]

    - delete an item by key::

        >>> foo_store.delete('002d050ee3ff6192dcbecc4e4b4457d7')
    """

    def __init__(self, client, collections, coltype, colname):
        self._client = client
        self._origin = _Collection(coltype, colname, collections._origin)
        proxy_methods(self._origin, self, [
            'create_writer', 'count',
            ('iter', 'iter_values'),
            ('iter_raw_json', 'iter_json'),
        ])
        # simplified version of _Proxy._wrap_iter_methods logic
        # to provide better support for filter param in iter methods
        for method in ['iter', 'iter_raw_json']:
            wrapped = wrap_kwargs(getattr(self, method), format_iter_filters)
            setattr(self, method, wrapped)

    def list(self, *args, **kwargs):
        """Convenient shortcut to list iter results.

        Please note that list() method can use a lot of memory and for a large
        amount of elements it's recommended to iterate through it via iter()
        method (all params and available filters are same for both methods).
        """
        return list(self.iter(*args, **kwargs))

    def get(self, key, *args, **kwargs):
        """Get item from collection by key.

        :param key: string item key
        :return: an item dictionary if exists
        """
        if key is None:
            raise ValueError("key cannot be None")
        return self._origin.get(key, *args, **kwargs)

    def set(self, *args, **kwargs):
        """Set item to collection by key.

        The method returns None (original method returns an empty generator).
        """
        self._origin.set(*args, **kwargs)

    def delete(self, keys):
        """Delete item(s) from collection by key(s).

        The method returns None (original method returns an empty generator).
        """
        if (not isinstance(keys, string_types) and
                not isinstance(keys, collections.Iterable)):
            raise ValueError("You should provide string key or iterable "
                             "object providing string keys")
        self._origin.delete(keys)

    def iter_raw_msgpack(self, requests_params=None, **apiparams):
        return self._origin._collections.iter_msgpack(
            self._origin.coltype, self._origin.colname,
            requests_params=requests_params, **apiparams)
