"""
Test utils module.
"""

from scrapinghub.hubstorage.utils import apipoll, sizeof_fmt


def test_sizeof_fmt():
    assert sizeof_fmt(1000) == '1000 B'
    assert sizeof_fmt(1024) == '1 KiB'
    assert sizeof_fmt(1024 * 1024) == '1 MiB'
    assert sizeof_fmt(1024 * 1024 + 100) == '1 MiB'
    assert sizeof_fmt(1024 * 1024 * 1024) == '1 GiB'


def test_apipoll_polls_until_result():
    results = iter([None, None, 'result'])

    def endpoint(**kwargs):
        return next(results)

    assert apipoll(endpoint, poll_wait=0) == 'result'


def test_apipoll_gives_up_on_max_poll():
    assert apipoll(lambda **kwargs: None, poll_wait=0, max_poll=0) is None
