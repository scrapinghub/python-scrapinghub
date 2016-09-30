"""
Test Activty
"""
from .hstestcase import HSTestCase
from six.moves import range


class ActivityTest(HSTestCase):

    def test_post_and_reverse_get(self):
        # make some sample data
        orig_data = [{u'foo': 42, u'counter': i} for i in range(20)]
        data1 = orig_data[:10]
        data2 = orig_data[10:]

        # put ordered data in 2 separate posts
        self.project.activity.post(data1)
        self.project.activity.post(data2)

        # read them back in reverse chronological order
        result = list(self.project.activity.list(count=20))
        self.assertEqual(len(result), 20)
        self.assertEqual(orig_data[::-1], result)

    def test_filters(self):
        self.project.activity.post({'c': i} for i in range(10))
        r = list(self.project.activity.list(filter='["c", ">", [5]]', count=2))
        self.assertEqual(r, [{'c': 9}, {'c': 8}])

    def test_timestamp(self):
        self.project.activity.add({'foo': 'bar'}, baz='qux')
        entry = next(self.project.activity.list(count=1, meta='_ts'))
        self.assertTrue(entry.pop('_ts', None))
        self.assertEqual(entry, {'foo': 'bar', 'baz': 'qux'})
