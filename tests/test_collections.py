"""
Test Collections
"""
import random
from six.moves import range
from contextlib import closing
from .hstestcase import HSTestCase
from .testutil import failing_downloader


def _mkitem():
    return dict(field1='value1', field2=['value2a', 'value2b'], field3=3, field4={'v4k': 'v4v'})

def random_collection_name():
    return "test_collection_%s" % random.randint(1, 1000000)

class CollectionsTest(HSTestCase):

    # For fixed tests (test_errors, test_data_download)
    test_collection_name = random_collection_name()

    def test_simple_count(self):
        coll_name = random_collection_name()
        test_item = dict(_mkitem())
        test_item['_key'] = 'a'

        collection = self.project.collections.new_store(coll_name)
        collection.set(test_item)
        assert collection.count() == 1

    def post_get_delete_test(self):
        test_item = _mkitem()
        item_to_send = dict(test_item)
        item_to_send['_key'] = test_key = 'insert_test_key'

        test_collections = [
            self.project.collections.new_store(self.test_collection_name),
            self.project.collections.new_cached_store(self.test_collection_name),
            self.project.collections.new_versioned_store(self.test_collection_name),
            self.project.collections.new_versioned_cached_store(self.test_collection_name),
        ]

        test_collections.extend(
            self.project.collections.new_collection(t, self.test_collection_name + 'b')
            for t in ('s', 'vs', 'cs', 'vcs'))

        for col in test_collections:
            col.set(item_to_send)
            returned_item = col.get(test_key)
            self.assertEqual(test_item, returned_item)
            col.delete(test_key)
            self.assertRaises(KeyError, col.get, test_key)

    def post_scan_test(self):
        col = self.project.collections.new_store(self.test_collection_name)

        # populate with 20 items
        test_item = _mkitem()
        last_key = None
        with closing(col.create_writer()) as writer:
            for i in range(20):
                test_item['_key'] = last_key = "post_scan_test%d" % i
                test_item['counter'] = i
                writer.write(test_item)

        # check last value is as expected
        returned_item = col.get(last_key)
        del test_item['_key']
        self.assertEqual(test_item, returned_item)

        # get all values starting with 1
        result = list(col.get(prefix='post_scan_test1'))
        # 1 & 10-19 = 11 items
        self.assertEqual(len(result), 11)

        # combining with normal filters
        result = list(col.get(filter='["counter", ">", [5]]', prefix='post_scan_test1'))
        # 10-19
        self.assertEqual(len(result), 10)

        # bulk delete
        col.delete('post_scan_test%d' % i for i in range(20))

        # test items removed (check first and last)
        self.assertRaises(KeyError, col.get, 'post_scan_test0')
        self.assertRaises(KeyError, col.get, last_key)

    def test_errors(self):
        col = self.project.collections.new_store(self.test_collection_name)
        self.assertRaises(KeyError, col.get, 'does_not_exist')
        self.assertRaises(ValueError, col.set, {'foo': 42})
        self.assertRaises(ValueError, col.set, {'_key': []})
        self.assertRaises(ValueError, col.set,
            {'_key': 'large_test', 'value': 'x' * 1024 ** 2})

    def test_data_download(self):
        col = self.project.collections.new_store(self.test_collection_name)
        items = []
        with closing(col.create_writer()) as writer:
            for i in range(20):
                test_item = _mkitem()
                test_item['_key'] = "test_data_download%d" % i
                test_item['counter'] = i
                writer.write(test_item)
                items.append(test_item)

        # check parameters are passed correctly
        downloaded = list(col.iter_values(prefix='test_data_download1'))
        self.assertEqual(len(downloaded), 11)

        # simulate network timeouts and download data
        with failing_downloader(self.project.collections):
            downloaded = list(col.iter_values(start='test_data_download1'))
            self.assertEqual(len(downloaded), 19)

    def test_invalid_collection_name(self):
        cols = self.project.collections
        self.assertRaises(ValueError, cols.new_collection, 'invalidtype', 'n')
        self.assertRaises(ValueError, cols.new_store, 'foo-bar')
        self.assertRaises(ValueError, cols.new_store, 'foo/bar')
        self.assertRaises(ValueError, cols.new_store, '/foo')
        self.assertRaises(ValueError, cols.create_writer, 'invalidtype', 'n')
        self.assertRaises(ValueError, cols.create_writer, 's', 'foo-bar')
