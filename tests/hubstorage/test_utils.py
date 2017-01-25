"""
Test utils module.
"""

from scrapinghub.hubstorage.utils import sizeof_fmt


def test_sizeof_fmt():
    assert sizeof_fmt(1000) == '1000 B'
    assert sizeof_fmt(1024) == '1 KiB'
    assert sizeof_fmt(1024 * 1024) == '1 MiB'
    assert sizeof_fmt(1024 * 1024 + 100) == '1 MiB'
    assert sizeof_fmt(1024 * 1024 * 1024) == '1 GiB'
