from contextlib import contextmanager
from collections.abc import Callable, Iterable, Iterator
from typing import Any
from requests import Timeout


@contextmanager
def failing_downloader(
    downloader: Any, N: int = 5, exc: type[BaseException] = Timeout,
    msg: str = 'test error',
) -> Iterator[Any]:
    # reduce wait times and simulate network timeout
    orig_iter = downloader._iter_lines
    orig_wait = downloader.RETRY_INTERVAL
    # generate a Timeout every N requests
    downloader._iter_lines = wrap_seq_to_fail(
        downloader._iter_lines, exc, N, msg)
    downloader.RETRY_INTERVAL = 0
    try:
        yield downloader
    finally:
        downloader._iter_lines = orig_iter
        downloader.RETRY_INTERVAL = orig_wait


def wrap_seq_to_fail(
    func: Callable[..., Iterable[Any]], exception: type[BaseException], N: int,
    *exc_args: Any,
) -> Callable[..., Iterator[Any]]:
    """Wrap a function that returns a sequence with failafter"""
    def _wrapper(*args: Any, **kwargs: Any) -> Iterator[Any]:
        seq = func(*args, **kwargs)
        return failafter(seq, exception, N, *exc_args)
    return _wrapper


def failafter(
    seq: Iterable[Any], exception: type[BaseException], N: int, *exc_args: Any,
) -> Iterator[Any]:
    """iterate over seq, and raise exception after N items"""
    for i, item in enumerate(seq):
        if i == N:
            raise exception(*exc_args)
        yield item
