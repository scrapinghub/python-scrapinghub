"""
Test Frontier
"""
from .hstestcase import HSTestCase


class FrontierTest(HSTestCase):

    def setUp(self):
        self._delete_slot()

    def tearDown(self):
        self._delete_slot()

    def _delete_slot(self):
        frontier = self.project.frontier
        frontier.delete_slot(self.frontier, self.slot)

    def _remove_all_ids(self):
        frontier = self.project.frontier
        ids = [batch['id'] for batch in frontier.read(self.frontier, self.slot)]
        frontier.delete(self.frontier, self.slot, ids)

    def _get_urls(self, batch):
        return [r[0] for r in batch['requests']]

    def test_add_read(self):
        frontier = self.project.frontier

        fps = [{'fp': '/'}]
        frontier.add(self.frontier, self.slot, fps)
        fps = [{'fp': '/index.html'}, {'fp': '/index2.html'}]
        frontier.add(self.frontier, self.slot, fps)
        frontier.flush()

        urls = [self._get_urls(batch) for batch
                in frontier.read(self.frontier, self.slot)]
        expected_urls = [[u'/', u'/index.html', u'/index2.html']]
        self.assertEqual(urls, expected_urls)

    def test_add_multiple_chunks(self):
        frontier = self.project.frontier
        old_count = frontier.newcount

        batch_size = 50
        fps1 = [{'fp': '/index_%s.html' % fp} for fp in range(0, batch_size)]
        frontier.add(self.frontier, self.slot, fps1)

        fps2 = [{'fp': '/index_%s.html' % fp} for fp in range(batch_size, batch_size * 2)]
        frontier.add(self.frontier, self.slot, fps2)

        fps3 = [{'fp': '/index_%s.html' % fp} for fp in range(batch_size * 2, batch_size * 3)]
        frontier.add(self.frontier, self.slot, fps3)
        frontier.flush()

        self.assertEqual(frontier.newcount, 150 + old_count)

        # insert repeated fingerprints
        fps4 = [{'fp': '/index_%s.html' % fp} for fp in range(0, batch_size)]
        frontier.add(self.frontier, self.slot, fps3)
        frontier.flush()

        # new count is the same
        self.assertEqual(frontier.newcount, 150 + old_count)

        # get first 100
        batches = list(frontier.read(self.frontier, self.slot, mincount=100))
        urls = [self._get_urls(batch) for batch in batches]
        expected_urls = [[fp['fp'] for fp in fps1 + fps2]]
        self.assertEqual(urls, expected_urls)

        # delete first 100
        ids = [batch['id'] for batch in batches]
        frontier.delete(self.frontier, self.slot, ids)

        # get remaining 50
        batches = list(frontier.read(self.frontier, self.slot))
        urls = [self._get_urls(batch) for batch in batches]
        expected_urls = [[fp['fp'] for fp in fps3]]
        self.assertEqual(urls, expected_urls)

    def test_add_big_chunk(self):
        frontier = self.project.frontier

        batch_size = 300
        fps1 = [{'fp': '/index_%s.html' % fp} for fp in range(0, batch_size)]
        frontier.add(self.frontier, self.slot, fps1)
        frontier.flush()

        # get first 100
        batches = list(frontier.read(self.frontier, self.slot, mincount=100))
        urls = [self._get_urls(batch) for batch in batches]
        expected_urls = [[fp['fp'] for fp in fps1[:100]]]
        self.assertEqual(urls, expected_urls)

        # delete first 100
        ids = [batch['id'] for batch in batches]
        frontier.delete(self.frontier, self.slot, ids)

        # get next 100
        batches = list(frontier.read(self.frontier, self.slot, mincount=100))
        urls = [self._get_urls(batch) for batch in batches]
        expected_urls = [[fp['fp'] for fp in fps1[100:200]]]
        self.assertEqual(urls, expected_urls)

        # delete next 100
        ids = [batch['id'] for batch in batches]
        frontier.delete(self.frontier, self.slot, ids)

        # get next 100
        batches = list(frontier.read(self.frontier, self.slot, mincount=100))
        urls = [self._get_urls(batch) for batch in batches]
        expected_urls = [[fp['fp'] for fp in fps1[200:300]]]
        self.assertEqual(urls, expected_urls)

    def test_add_extra_params(self):
        frontier = self.project.frontier

        qdata = {"a": 1, "b": 2, "c": 3}
        fps = [{'fp': '/', "qdata": qdata}]
        frontier.add(self.frontier, self.slot, fps)
        frontier.flush()

        expected_request = [[u'/', {u'a': 1, u'c': 3, u'b': 2}]]
        batches = list(frontier.read(self.frontier, self.slot))
        request = batches[0]['requests']
        self.assertEqual(request, expected_request)

