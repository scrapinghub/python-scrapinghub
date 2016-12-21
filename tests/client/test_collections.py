"""
Test Collections
"""
import random
from contextlib import closing

import pytest
from six.moves import range

from .conftest import TEST_COLLECTION_NAME


def _mkitem():
    return dict(field1='value1', field2=['value2a', 'value2b'],
                field3=3, field4={'v4k': 'v4v'})


def test_simple_count(project, collection):
    test_item = dict(_mkitem())
    test_item['_key'] = 'a'

    collection.set(test_item)
    assert collection.count() == 1


def test_post_get_delete(project):
    test_item = _mkitem()
    item_to_send = dict(test_item)
    item_to_send['_key'] = test_key = 'insert_test_key'

    test_collections = [
        project.collections.new_store(TEST_COLLECTION_NAME),
        project.collections.new_cached_store(TEST_COLLECTION_NAME),
        project.collections.new_versioned_store(TEST_COLLECTION_NAME),
        project.collections.new_versioned_cached_store(TEST_COLLECTION_NAME),
    ]

    test_collections.extend(
        project.collections.new_collection(t, TEST_COLLECTION_NAME + 'b')
        for t in ('s', 'vs', 'cs', 'vcs'))

    for col in test_collections:
        col.set(item_to_send)
        returned_item = col.get(test_key)
        assert test_item == returned_item
        col.delete(test_key)
        with pytest.raises(KeyError):
            col.get(test_key)


def test_post_scan(project, collection):
    # populate with 20 items
    test_item = _mkitem()
    last_key = None
    with closing(collection.create_writer()) as writer:
        for i in range(20):
            test_item['_key'] = last_key = "post_scan_test%d" % i
            test_item['counter'] = i
            writer.write(test_item)

    # check last value is as expected
    returned_item = collection.get(last_key)
    del test_item['_key']
    assert test_item == returned_item

    # get all values starting with 1
    result = list(collection.get(prefix='post_scan_test1'))
    # 1 & 10-19 = 11 items
    assert len(result) == 11

    # combining with normal filters
    result = list(collection.get(filter='["counter", ">", [5]]',
                          prefix='post_scan_test1'))
    # 10-19
    assert len(result) == 10

    # bulk delete
    collection.delete('post_scan_test%d' % i for i in range(20))

    # test items removed (check first and last)
    with pytest.raises(KeyError):
        collection.get('post_scan_test0')
    with pytest.raises(KeyError):
        collection.get(last_key)


def test_errors_bad_key(collection):
    with pytest.raises(KeyError):
        collection.get('does_not_exist')


@pytest.mark.parametrize('testarg', [
        {'foo': 42},
        {'_key': []},
        {'_key': 'large_test', 'value': 'x' * 1024 ** 2},
])
def test_errors(collection, testarg):
    with pytest.raises(ValueError):
        collection.set(testarg)


def test_data_download(project, collection):
    items = []
    with closing(collection.create_writer()) as writer:
        for i in range(20):
            test_item = _mkitem()
            test_item['_key'] = "test_data_download%d" % i
            test_item['counter'] = i
            writer.write(test_item)
            items.append(test_item)

    # check parameters are passed correctly
    downloaded = list(collection.iter(prefix='test_data_download1'))
    assert len(downloaded) == 11

    downloaded = list(collection.iter(start='test_data_download1'))
    assert len(downloaded) == 19


def test_invalid_collection_name(project):
    cols = project.collections
    for method, args in [
            (cols.new_collection, ('invalidtype', 'n')),
            (cols.new_store, ('foo-bar',)),
            (cols.new_store, ('foo/bar',)),
            (cols.new_store, ('/foo',)),
            (cols.create_writer, ('invalidtype', 'n')),
            (cols.create_writer, ('s', 'foo-bar'))]:
        with pytest.raises(ValueError):
            method(*args)
