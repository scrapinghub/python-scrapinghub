from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from .resourcetype import ResourceType

# TODO: remove backwards compatible methods


class Activity(ResourceType):

    resource_type = 'activity'

    def list(self, **params: Any) -> Iterator[Any]:
        return self.apiget(params=params)
    get = list

    def post(self, _value: Any, **params: Any) -> Iterator[Any]:
        return self.apipost(jl=_value, params=params)

    def add(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        entry = dict(*args, **kwargs)
        return self.post(entry)
