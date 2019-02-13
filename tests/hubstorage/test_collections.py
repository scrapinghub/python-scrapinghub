"""
Test Collections
"""
import random
from contextlib import closing

import pytest
from scrapinghub import HubstorageClient
from six.moves import range

from .conftest import TEST_COLLECTION_NAME
from .testutil import failing_downloader


def _mkitem():
    return dict(field1='value1', field2=['value2a', 'value2b'],
                field3=3, field4={'v4k': 'v4v'})


def test_simple_count(hsproject, hscollection):
    test_item = dict(_mkitem())
    test_item['_key'] = 'a'

    hscollection.set(test_item)
    assert hscollection.count() == 1


def post_get_delete_test(hsproject):
    test_item = _mkitem()
    item_to_send = dict(test_item)
    item_to_send['_key'] = test_key = 'insert_test_key'

    test_collections = [
        hsproject.collections.new_store(TEST_COLLECTION_NAME),
        hsproject.collections.new_cached_store(TEST_COLLECTION_NAME),
        hsproject.collections.new_versioned_store(TEST_COLLECTION_NAME),
        hsproject.collections.new_versioned_cached_store(TEST_COLLECTION_NAME),
    ]

    test_collections.extend(
        hsproject.collections.new_collection(t, TEST_COLLECTION_NAME + 'b')
        for t in ('s', 'vs', 'cs', 'vcs'))

    for col in test_collections:
        col.set(item_to_send)
        returned_item = col.get(test_key)
        assert test_item == returned_item
        col.delete(test_key)
        with pytest.raises(KeyError):
            col.get(test_key)


def post_scan_test(hsproject, hscollection):
    # populate with 20 items
    test_item = _mkitem()
    last_key = None
    with closing(hscollection.create_writer()) as writer:
        for i in range(20):
            test_item['_key'] = last_key = "post_scan_test%d" % i
            test_item['counter'] = i
            writer.write(test_item)

    # check last value is as expected
    returned_item = hscollection.get(last_key)
    del test_item['_key']
    assert test_item == returned_item

    # get all values starting with 1
    result = list(hscollection.get(prefix='post_scan_test1'))
    # 1 & 10-19 = 11 items
    assert len(result) == 11

    # combining with normal filters
    result = list(hscollection.get(filter='["counter", ">", [5]]',
                          prefix='post_scan_test1'))
    # 10-19
    assert len(result) == 10

    # bulk delete
    hscollection.delete('post_scan_test%d' % i for i in range(20))

    # test items removed (check first and last)
    with pytest.raises(KeyError):
        hscollection.get('post_scan_test0')
    with pytest.raises(KeyError):
        hscollection.get(last_key)


def test_errors_bad_key(hscollection, json_and_msgpack):
    with pytest.raises(KeyError):
        hscollection.get('does_not_exist')


@pytest.mark.parametrize('testarg', [
        {'foo': 42},
        {'_key': []},
        {'_key': 'large_test', 'value': 'x' * 1024 ** 2},
])
def test_errors(hscollection, testarg):
    with pytest.raises(ValueError):
        hscollection.set(testarg)


def test_data_download(hsproject, hscollection):
    items = []
    with closing(hscollection.create_writer()) as writer:
        for i in range(20):
            test_item = _mkitem()
            test_item['_key'] = "test_data_download%d" % i
            test_item['counter'] = i
            writer.write(test_item)
            items.append(test_item)

    # check parameters are passed correctly
    downloaded = list(hscollection.iter_values(prefix='test_data_download1'))
    assert len(downloaded) == 11

    # simulate network timeouts and download data
    with failing_downloader(hsproject.collections):
        downloaded = list(hscollection.iter_values(start='test_data_download1'))
        assert len(downloaded) == 19


def test_invalid_collection_name(hsproject):
    cols = hsproject.collections
    for method, args in [
            (cols.new_collection, ('invalidtype', 'n')),
            (cols.new_store, ('foo-bar',)),
            (cols.new_store, ('foo/bar',)),
            (cols.new_store, ('/foo',)),
            (cols.create_writer, ('invalidtype', 'n')),
            (cols.create_writer, ('s', 'foo-bar'))]:
        with pytest.raises(ValueError):
            method(*args)


@pytest.mark.parametrize('path,expected_result', [
        ('s/foo', True),
        ('s/foo/', True),
        (('s', 'foo'), True),
        ('s/foo/bar', True),
        ('s/foo/bar/', True),
        (('s', 'foo', 'bar'), True),
        ('vs/foo/bar/', True),
        ('cs/foo/bar/', True),
        ('vcs/foo/bar/', True),
        ('s/foo/scan', True),
        ('s/foo/bar/baz', False),
        ('s/foo/count', False),
        (('s', 'foo', 'count'), False),
        ('x/foo', False),
        (('x', 'foo'), False),
        ('list', False),
        (None, False),
])
def test_allows_msgpack(hsclient, path, expected_result, json_and_msgpack):
    collections = hsclient.get_project(2222000).collections
    assert collections._allows_mpack(path) is (hsclient.use_msgpack and expected_result)


def test_truncate(hscollection):
    # populate with 20 items
    test_item = _mkitem()
    with closing(hscollection.create_writer()) as writer:
        for i in range(20):
            test_item['_key'] = "my_key_%d" % i
            test_item['counter'] = i
            writer.write(test_item)

    assert len(list(hscollection.iter_values(prefix='my_key'))) == 20

    hscollection.truncate()
    assert len(list(hscollection.iter_values(prefix='my_key'))) == 0
