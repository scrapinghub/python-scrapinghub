from __future__ import absolute_import, annotations

import six
import json
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ..hubstorage import ValueTooLarge as _ValueTooLarge
from ..hubstorage.resourcetype import (
    DownloadableResource,
    ItemsResourceType,
    MappingResourceType,
    ResourceType,
)
from .utils import update_kwargs
from .exceptions import ValueTooLarge

if TYPE_CHECKING:
    from ..hubstorage import HubstorageClient
    from . import ScrapinghubClient

_OriginT = TypeVar('_OriginT', bound=ResourceType)
_ItemsOriginT = TypeVar('_ItemsOriginT', bound=ItemsResourceType)
_DownloadableOriginT = TypeVar('_DownloadableOriginT',
                               bound=DownloadableResource)
_MappingOriginT = TypeVar('_MappingOriginT', bound=MappingResourceType)


class _Proxy(Generic[_OriginT]):
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

    def __init__(self, cls: Callable[[HubstorageClient, str], _OriginT],
                 client: ScrapinghubClient, key: str) -> None:
        self.key = key
        self._client = client
        self._origin = cls(client._hsclient, key)

    def list(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Convenient shortcut to list iter results.

        Please note that :meth:`list` method can use a lot of memory and for a
        large amount of elements it's recommended to iterate through it via
        :meth:`iter` method (all params and available filters are same for both
        methods).
        """
        return list(self.iter(*args, **kwargs))  # type: ignore[attr-defined]

    def _modify_iter_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """A helper to modify iter*() params on-the-fly.

        The method is internal and should be redefined in subclasses.

        :param params: a dictionary with input parameters.
        :return: an updated dictionary with parameters.
        :rtype: :class:`dict`
        """
        return _format_iter_filters(params)


class _ItemsResourceProxy(_Proxy[_ItemsOriginT]):

    def get(self, key: str | int, **params: Any) -> Any:
        """Get element from collection.

        :param key: element key.
        :return: a dictionary with element data.
        :rtype: :class:`dict`
        """
        return self._origin.get(key, **params)

    def write(self, item: Any) -> int:
        """Write new element to collection.

        :param item: element data dict to write.
        """
        try:
            return self._origin.write(item)
        except _ValueTooLarge as exc:
            raise ValueTooLarge(str(exc))

    def iter(self, _key: str | None = None, count: int | None = None,
             **params: Any) -> Iterator[Any]:
        """Iterate over elements in collection.

        :param count: limit amount of elements.
        :return: a generator object over a list of element dictionaries.
        :rtype: :class:`types.GeneratorType[dict]`
        """
        update_kwargs(params, count=count)
        params = self._modify_iter_params(params)
        return self._origin.list(_key, **params)

    def flush(self) -> None:
        """Flush data from writer threads."""
        self._origin.flush()

    def stats(self) -> Any:
        """Get resource stats.

        :return: a dictionary with stats data.
        :rtype: :class:`dict`
        """
        return self._origin.stats()

    def close(self, block: bool = True) -> None:
        """Close writers one-by-one."""
        self._origin.close(block)


class _DownloadableProxyMixin(_Proxy[_DownloadableOriginT]):

    def iter(self, _path: str | None = None, count: int | None = None,
             requests_params: dict[str, Any] | None = None,
             **apiparams: Any) -> Iterator[Any]:
        """A general method to iterate through elements.

        :param count: limit amount of elements.
        :return: an iterator over elements list.
        :rtype: :class:`collections.abc.Iterable`
        """
        update_kwargs(apiparams, count=count)
        apiparams = self._modify_iter_params(apiparams)
        drop_key = '_key' not in (apiparams.get('meta') or [])
        for entry in self._origin.iter_values(
            _path, requests_params, **apiparams
        ):
            if drop_key and '_key' in entry:
                entry.pop('_key')
            yield entry


class _MappingProxy(_Proxy[_MappingOriginT]):
    """A helper class to support basic get/set interface for dict-like
    collections of elements.
    """

    def get(self, key: str) -> Any:
        """Get element value by key.

        :param key: a string key
        """
        return next(self._origin.apiget(key))

    def set(self, key: str, value: Any) -> None:
        """Set element value.

        :param key: a string key
        :param value: new value to set for the key
        """
        self._origin.apipost(key, data=json.dumps(value), is_idempotent=True)

    def update(self, values: dict[str, Any]) -> None:
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

    def delete(self, key: str) -> None:
        """Delete element by key.

        :param key: a string key
        """
        self._origin.apidelete(key)

    def iter(self) -> Iterator[tuple[str, Any]]:
        """Iterate through key/value pairs.

        :return: an iterator over key/value pairs.
        :rtype: :class:`collections.abc.Iterable`
        """
        return six.iteritems(next(self._origin.apiget()))


def _format_iter_filters(params: dict[str, Any]) -> dict[str, Any]:
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
