from contextlib import closing

import pytest
from six.moves import range

from scrapinghub.client.exceptions import BadRequest
from scrapinghub.client.exceptions import NotFound
from scrapinghub.client.exceptions import ValueTooLarge

from .conftest import TEST_COLLECTION_NAME


def _mkitem():
    return dict(field1='value1', field2=['value2a', 'value2b'],
                field3=3, field4={'v4k': 'v4v'})


def test_collections_list(project):
    # create/check test collections
    project.collections.get_store(TEST_COLLECTION_NAME),
    project.collections.get_cached_store(TEST_COLLECTION_NAME),
    project.collections.get_versioned_store(TEST_COLLECTION_NAME),
    project.collections.get_versioned_cached_store(TEST_COLLECTION_NAME),
    collections = project.collections.list()
    assert isinstance(collections, list)
    assert len(collections) >= 4
    for coltype in ('s', 'vs', 'cs', 'vcs'):
        assert {'name': TEST_COLLECTION_NAME, 'type': coltype} in collections


def test_simple_count(project, collection):
    test_item = dict(_mkitem())
    test_item['_key'] = 'a'

    collection.set(test_item)
    assert collection.count() == 1


def test_post_get_delete(project, json_and_msgpack):
    test_item = _mkitem()
    item_to_send = dict(test_item)
    item_to_send['_key'] = test_key = 'insert_test_key'

    test_collections = [
        project.collections.get_store(TEST_COLLECTION_NAME),
        project.collections.get_cached_store(TEST_COLLECTION_NAME),
        project.collections.get_versioned_store(TEST_COLLECTION_NAME),
        project.collections.get_versioned_cached_store(TEST_COLLECTION_NAME),
    ]

    test_collections.extend(
        project.collections.get(t, TEST_COLLECTION_NAME + 'b')
        for t in ('s', 'vs', 'cs', 'vcs'))

    for col in test_collections:
        col.set(item_to_send)
        returned_item = col.get(test_key)
        assert test_item == returned_item
        col.delete(test_key)
        with pytest.raises(NotFound):
            col.get(test_key)


def test_post_scan(project, collection, json_and_msgpack):
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

    # get requires key field
    with pytest.raises(TypeError):
        collection.get()

    result = collection.get('post_scan_test2')
    assert isinstance(result, dict)
    assert result['counter'] == 2

    # get all values starting with 1
    result = list(collection.iter(prefix='post_scan_test1'))
    # 1 & 10-19 = 11 items
    assert len(result) == 11

    # combining with normal filters
    result = list(collection.iter(filter='["counter", ">", [5]]',
                                  prefix='post_scan_test1'))
    # 10-19
    assert len(result) == 10

    # bulk delete
    collection.delete('post_scan_test%d' % i for i in range(20))

    # test items removed (check first and last)
    with pytest.raises(NotFound):
        collection.get('post_scan_test0')
    with pytest.raises(NotFound):
        collection.get(last_key)


def test_errors_bad_key(collection, json_and_msgpack):
    with pytest.raises(NotFound):
        collection.get('does_not_exist')


@pytest.mark.parametrize('testarg', [
        {'foo': 42},
        {'_key': []},
])
def test_errors(collection, testarg):
    with pytest.raises(BadRequest):
        collection.set(testarg)


def test_entity_too_large(collection):
    with pytest.raises(ValueTooLarge):
        collection.set({'_key': 'large_test', 'value': 'x' * 1024 ** 2})


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
            (cols.get, ('invalidtype', 'n')),
            (cols.get_store, ('foo-bar',)),
            (cols.get_store, ('foo/bar',)),
            (cols.get_store, ('/foo',))]:
        with pytest.raises(ValueError):
            method(*args)


def test_truncate(collection):
    # populate with 20 items
    test_item = _mkitem()
    with closing(collection.create_writer()) as writer:
        for i in range(20):
            test_item['_key'] = "my_key_%d" % i
            test_item['counter'] = i
            writer.write(test_item)

    assert len(list(collection.iter(prefix='my_key'))) == 20

    collection.truncate()
    assert len(list(collection.iter(prefix='my_key'))) == 0
