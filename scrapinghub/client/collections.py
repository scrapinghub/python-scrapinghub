from __future__ import absolute_import
import collections

from six import string_types

from ..hubstorage.collectionsrt import Collection as _Collection

from .utils import (
    _Proxy, format_iter_filters, proxy_methods, wrap_kwargs, update_kwargs,
)


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

    def get(self, type_, name):
        """Base method to get a collection with a given type and name.

        :param type_: a collection type string.
        :param name: a collection name string.
        :return: :class:`Collection` object.
        :rtype: Collection
        """
        self._origin._validate_collection(type_, name)
        return Collection(self._client, self, type_, name)

    def get_store(self, name):
        """Method to get a store collection by name.

        :param name: a collection name string.
        :return: :class:`Collection` object.
        :rtype: Collection
        """
        return self.get('s', name)

    def get_cached_store(self, name):
        """Method to get a cashed-store collection by name.

        The collection type means that items expire after a month.

        :param name: a collection name string.
        :return: :class:`Collection` object.
        :rtype: Collection
        """
        return self.get('cs', name)

    def get_versioned_store(self, name):
        """Method to get a versioned-store collection by name.

        The collection type retains up to 3 copies of each item.

        :param name: a collection name string.
        :return: :class:`Collection` object.
        :rtype: Collection
        """
        return self.get('vs', name)

    def get_versioned_cached_store(self, name):
        """Method to get a versioned-cached-store collection by name.

        Multiple copies are retained, and each one expires after a month.

        :param name: a collection name string.
        :return: :class:`Collection` object.
        :rtype: Collection
        """
        return self.get('vcs', name)

    def iter(self):
        """Iterate through collections of a project.

        :return: an iterator over collections list where each collection is
            represented by a dictionary with ('name','type') fields.
        :rtype: collections.Iterable[dict]
        """
        return self._origin.apiget('list')

    def list(self):
        """List collections of a project.

        :return: a list of collections where each collection is
            represented by a dictionary with ('name','type') fields.
        :rtype: list[dict]
        """
        return list(self.iter())


class Collection(object):
    """Representation of a project collection object.

    Not a public constructor: use :class:`Collections` instance to get a
    :class:`Collection` instance. See :meth:`Collections.get_store` and
    similar methods.

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
        [{'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'}]

    - filter by multiple keys, only values for keys that exist will be returned::

        >>> foo_store.list(key=['002d050ee3ff6192dcbecc4e4b4457d7', 'blah'])
        [{'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'}]

    - delete an item by key::

        >>> foo_store.delete('002d050ee3ff6192dcbecc4e4b4457d7')
    """

    def __init__(self, client, collections, type_, name):
        self._client = client
        self._origin = _Collection(type_, name, collections._origin)
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

    def list(self, key=None, prefix=None, prefixcount=None, startts=None,
             endts=None, requests_params=None, **params):
        """Convenient shortcut to list iter results.

        Please note that list() method can use a lot of memory and for a large
        amount of elements it's recommended to iterate through it via iter()
        method (all params and available filters are same for both methods).

        :param key: a string key or a list of keys to filter with.
        :param prefix: a string prefix to filter items.
        :param prefixcount: maximum number of values to return per prefix.
        :param startts: UNIX timestamp at which to begin results.
        :param endts: UNIX timestamp at which to end results.
        :param requests_params: (optional) a dict with optional requests params.
        :param \*\*params: (optional) additional query params for the request.
        :return: a list of items where each item is represented with a dict.
        :rtype: list[dict]

        # FIXME there should be similar docstrings for iter/iter_raw_json
        # but as we proxy them as-is, it's not in place, should be improved
        """
        update_kwargs(params, key=key, prefix=prefix, prefixcount=prefixcount,
                      startts=startts, endts=endts,
                      requests_params=requests_params)
        return list(self.iter(requests_params=None, **params))

    def get(self, key, **params):
        """Get item from collection by key.

        :param key: string item key.
        :param \*\*params: (optional) additional query params for the request.
        :return: an item dictionary if exists.
        :rtype: dict
        """
        if key is None:
            raise ValueError("key cannot be None")
        return self._origin.get(key, **params)

    def set(self, value):
        """Set item to collection by key.

        :param value: a dict representing a collection item.

        The method returns None (original method returns an empty generator).
        """
        self._origin.set(value)

    def delete(self, keys):
        """Delete item(s) from collection by key(s).

        :param keys: a single key or a list of keys.

        The method returns None (original method returns an empty generator).
        """
        if (not isinstance(keys, string_types) and
                not isinstance(keys, collections.Iterable)):
            raise ValueError("You should provide string key or iterable "
                             "object providing string keys")
        self._origin.delete(keys)

    def iter_raw_msgpack(self, key=None, prefix=None, prefixcount=None,
                         startts=None, endts=None, requests_params=None,
                         **params):
        """A method to iterate through raw msgpack-ed items.
        Can be convenient if data is needed in same msgpack format.

        :param key: a string key or a list of keys to filter with.
        :param prefix: a string prefix to filter items.
        :param prefixcount: maximum number of values to return per prefix.
        :param startts: UNIX timestamp at which to begin results.
        :param endts: UNIX timestamp at which to end results.
        :param requests_params: (optional) a dict with optional requests params.
        :param \*\*params: (optional) additional query params for the request.
        :return: an iterator over items list packed with msgpack.
        :rtype: collections.Iterable[bytes]
        """
        update_kwargs(params, key=key, prefix=prefix, prefixcount=prefixcount,
                      startts=startts, endts=endts,
                      requests_params=requests_params)
        return self._origin._collections.iter_msgpack(
            self._origin.coltype, self._origin.colname, **params)
