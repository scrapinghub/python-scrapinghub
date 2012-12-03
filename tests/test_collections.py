"""
Test Collections
"""
import json, random
from unittest import TestCase
from hstestcase import HSTestCase


def _mkitem():
    return dict(field1='value1', field2=['value2a', 'value2b'], field3=3, field4={'v4k': 'v4v'})


class CollectionsTest(HSTestCase):

    def setUp(self):
        super(CollectionsTest, self).setUp()
        self.test_collection_name = "test_collection_%s" % random.randint(1, 1000000)
        self.collections = self.hsclient.get_collections(self.projectid)

    def post_get_delete_test(self):
        test_item = _mkitem()
        item_to_send = dict(test_item)
        item_to_send['_key'] = test_key = 'insert_test_key'

        test_collections = [
            self.collections.new_store(self.test_collection_name),
            self.collections.new_cached_store(self.test_collection_name),
            self.collections.new_versioned_store(self.test_collection_name),
            self.collections.new_versioned_cached_store(self.test_collection_name),
        ]

        for col in test_collections:
            col.set(item_to_send)
            returned_item = col.get(test_key)
            self.assertEqual(test_item, returned_item)
            col.delete(test_key)
            self.assertRaises(KeyError, col.get, test_key)

    def post_scan_test(self):
        col = self.collections.new_store(self.test_collection_name)

        # populate with 20 items
        test_item = _mkitem()
        last_key = None
        for i in xrange(20):
            test_item['_key'] = last_key = "post_scan_test%d" % i
            test_item['counter'] = i
            col.set(test_item)

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
        col.delete('post_scan_test%d' % i for i in xrange(20))

        # test items removed (check first and last)
        self.assertRaises(KeyError, col.get, 'post_scan_test0')
        self.assertRaises(KeyError, col.get, last_key)

    def test_errors(self):
        col = self.collections.new_store(self.test_collection_name)
        self.assertRaises(KeyError, col.get, 'does_not_exist')
        self.assertRaises(ValueError, col.set, {'foo': 42})
        self.assertRaises(ValueError, col.set, {'_key': []})
        self.assertRaises(ValueError, col.set, {'_key': 'large_test', 'value': 'x'*1024**2})
