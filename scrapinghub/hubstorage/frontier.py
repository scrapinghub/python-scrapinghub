
from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

import requests

from .resourcetype import ResourceType
from .utils import urlpathjoin

if TYPE_CHECKING:
    from .batchuploader import _BatchWriter
    from .client import HubstorageClient


class Frontier(ResourceType):

    resource_type = 'hcf'

    client: HubstorageClient

    batch_size = 5000
    batch_qsize = 6000  # defaults to twice batch_size if None
    batch_start = 0
    batch_interval = 60.0
    batch_append = False
    batch_content_encoding = 'identity'

    def __init__(self, *a: Any, **kw: Any) -> None:
        self._writers: dict[tuple[str, str], _BatchWriter] = {}  # dict of writers indexed by (frontier, slot)
        self.newcount = 0
        super(Frontier, self).__init__(*a, **kw)

    def _get_writer(self, frontier: str, slot: str) -> _BatchWriter:
        key = (frontier, slot)
        writer = self._writers.get(key)
        if not writer:
            writer = self.client.batchuploader.create_writer(
                url=urlpathjoin(self.url, frontier, 's', slot),
                auth=self.auth,
                size=self.batch_size,
                start=self.batch_start,
                interval=self.batch_interval,
                qsize=self.batch_qsize,
                content_encoding=self.batch_content_encoding,
                callback=self._writer_callback
            )
            self._writers[key] = writer
        return writer

    def _writer_callback(self, response: requests.Response | None) -> None:
        assert response is not None
        self.newcount += response.json()["newcount"]

    def close(self, block: bool = True) -> None:
        for writer in self._writers.values():
            writer.close(block=block)

    def flush(self) -> None:
        for writer in self._writers.values():
            writer.flush()

    def add(self, frontier: str, slot: str, fps: Iterable[Any]) -> None:
        writer = self._get_writer(frontier, slot)
        for fp in fps:
            writer.write(fp)

    def read(self, frontier: str, slot: str,
             mincount: int | None = None) -> Iterator[Any]:
        params: dict[str, Any] = {}
        if mincount is not None:
            params['mincount'] = mincount
        return self.apiget((frontier, 's', slot, 'q'), params=params)

    def delete(self, frontier: str, slot: str, ids: Iterable[str]) -> None:
        self.apipost((frontier, 's', slot, 'q/deleted'), jl=ids, is_idempotent=True)

    def delete_slot(self, frontier: str, slot: str) -> None:
        self.apidelete((frontier, 's', slot))
