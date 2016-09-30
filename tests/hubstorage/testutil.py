from contextlib import contextmanager
from requests import Timeout


@contextmanager
def failing_downloader(downloader, N=5, exc=Timeout, msg='test error'):
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


def wrap_seq_to_fail(func, exception, N, *exc_args):
    """Wrap a function that returns a sequence with failafter"""
    def _wrapper(*args, **kwargs):
        seq = func(*args, **kwargs)
        return failafter(seq, exception, N, *exc_args)
    return _wrapper


def failafter(seq, exception, N, *exc_args):
    """iterate over seq, and raise exception after N items"""
    for i, item in enumerate(seq):
        if i == N:
            raise exception(*exc_args)
        yield item
