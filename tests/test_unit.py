import StringIO
import json
import unittest

from scrapinghub import (
    Connection,
    json,
    urlencode,
    to_str,
)

class MockResponse(object):
    def __init__(self, client, content):
        self.client = client
        self.content = content
        self.orig_urlopen = None

    def _urlopen(self, url, data, request_headers):
        self.url = url
        self.data = data
        self.headers = request_headers
        return StringIO.StringIO(self.content)

    def __enter__(self):
        self.orig_urlopen = self.client._urlopen
        self.client._urlopen = self._urlopen
        return self

    def __exit__(self, *args):
        self.client._urlopen = self.orig_urlopen
        self.orig_urlopen = None


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        self.client = Connection('john', 'doe', url='http://server/api/')

    def test_initialization(self):
        self.assertEqual(self.client.url, 'http://server/api/')
        self.assertEqual(str(self.client), 'Connection(http://server/api/)')
        self.assertEqual(self.client._request_headers['Authorization'], 'Basic am9objpkb2U=')

    def test_project_names(self):
        content = json.dumps(dict(status='ok', projects=['foo', 'bar']))
        with MockResponse(self.client, content) as mock:
            self.assertEqual(self.client.project_names(), ['foo', 'bar'])

            self.assertEqual(mock.url,
                             'http://server/api/scrapyd/listprojects.json')
            self.assertEqual(mock.data, None)


class ProjectTest(unittest.TestCase):

    def setUp(self):
        self.client = Connection('john', 'doe', url='http://server/api/')

    def test_project_access(self):
        p1 = self.client['foo']
        p2 = self.client['bar']
        self.assertEqual(str(p1), 'Project(Connection(http://server/api/), foo)')
        self.assertEqual(str(p2), 'Project(Connection(http://server/api/), bar)')

    def test_schedule(self):
        content = json.dumps(dict(status='ok', jobid='a1'))

        p = self.client['foo']
        with MockResponse(self.client, content) as mock:
            result = p.schedule('foo', arg='bar')
            self.assertEqual(result, 'a1')
            self.assertEqual(mock.url,
                             'http://server/api/schedule.json')
            self.assertEqual(mock.data,
                             'project=foo&spider=foo&arg=bar')

    def test_jobs_empty(self):
        content = '\n'.join([
            json.dumps(dict(status='ok', total=0)),
        ])

        p = self.client['foo']
        with MockResponse(self.client, content) as mock:
            jset = p.jobs()
            self.assertEqual(str(jset),
                             'JobSet(Project(Connection(http://server/api/), foo), )')
            self.assertEqual(list(jset), [])

            self.assertEqual(mock.url,
                             'http://server/api/jobs/list.jl?project=foo')
            self.assertEqual(mock.data, None)

    def test_jobs(self):
        content = '\n'.join([
            json.dumps(dict(status='ok', total=2)),
            json.dumps(dict(id='a1')),
            json.dumps(dict(id='a2'))
        ])

        p = self.client['foo']
        with MockResponse(self.client, content) as mock:
            jset = p.jobs(has_tag='t1', count='100')
            self.assertEqual(str(jset),
                             'JobSet(Project(Connection(http://server/api/), foo), count=100, has_tag=t1)')
            jobs = list(jset)
            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0].id, 'a1')
            self.assertEqual(jobs[1].id, 'a2')

            self.assertEqual(mock.url,
                             'http://server/api/jobs/list.jl?count=100&project=foo&has_tag=t1')
            self.assertEqual(mock.data, None)

    def test_jobs_count(self):
        content = json.dumps(dict(status='ok', total=10))

        p = self.client['foo']
        with MockResponse(self.client, content) as mock:
            total = p.jobs(has_tag='t1').count()
            self.assertEqual(total, 10)

            self.assertEqual(mock.url,
                             'http://server/api/jobs/count.json?project=foo&has_tag=t1')
            self.assertEqual(mock.data, None)


    def test_jobs_update(self):
        content = json.dumps(dict(status='ok', count=2))

        p = self.client['foo']
        with MockResponse(self.client, content) as mock:
            result = p.jobs(has_tag='t1').update(add_tag='t1')
            self.assertEqual(result, 2)

            self.assertEqual(mock.url,
                             'http://server/api/jobs/update.json')
            self.assertEqual(mock.data,
                             'project=foo&add_tag=t1&has_tag=t1')

    def test_single_job(self):
        content = '\n'.join([
            json.dumps(dict(status='ok', total=2)),
            json.dumps(dict(id='a1')),
        ])

        p = self.client['foo']
        with MockResponse(self.client, content) as mock:
            job = p.job('a1')
            self.assertEqual(job.id, 'a1')

            self.assertEqual(mock.url,
                             'http://server/api/jobs/list.jl?count=1&project=foo&job=a1')
            self.assertEqual(mock.data, None)

        # update job
        content = json.dumps(dict(status='ok', count=1))
        with MockResponse(self.client, content) as mock:
            result = job.update(add_tag='t1')
            self.assertEqual(result, 1)

            self.assertEqual(mock.url,
                             'http://server/api/jobs/update.json')
            self.assertEqual(mock.data,
                             'project=foo&job=a1&add_tag=t1')

        # delete job
        content = json.dumps(dict(status='ok', count=1))
        with MockResponse(self.client, content) as mock:
            result = job.delete()
            self.assertEqual(result, 1)

            self.assertEqual(mock.url,
                             'http://server/api/jobs/delete.json')
            self.assertEqual(mock.data,
                             'project=foo&job=a1')

        # fetch items
        content = '\n'.join([
            json.dumps(dict(_id='i1')),
            json.dumps(dict(_id='i2')),
        ])
        with MockResponse(self.client, content) as mock:
            result = job.items()
            items = list(result)
            self.assertEqual(len(items), 2)
            self.assertEqual(items[0]['_id'], 'i1')
            self.assertEqual(items[1]['_id'], 'i2')

            self.assertEqual(mock.url,
                             'http://server/api/items.jl?project=foo&job=a1')
            self.assertEqual(mock.data, None)


class UtilsTest(unittest.TestCase):

    def test_urlencode(self):
        self.assertEqual(urlencode({}), '')
        self.assertEqual(urlencode(()), '')
        self.assertEqual(urlencode(dict(foo='bar', bar='foo')),
                         'foo=bar&bar=foo')
        self.assertEqual(urlencode([('foo', 'bar'), ('bar', 'foo')]),
                         'foo=bar&bar=foo')

    def test_urlencode_unicode(self):
        self.assertEqual(urlencode([(u'\xf1', u'\u20ac')]),
                         '%C3%B1=%E2%82%AC')

    def test_urlencode_no_doseq(self):
        self.assertEqual(urlencode(dict(foo=[1])),
                                   'foo=%5B%271%27%5D')

    def test_urlencode_doseq(self):
        self.assertEqual(urlencode(dict(foo=[1,2,3]), True),
                         'foo=1&foo=2&foo=3')

    def test_urlencode_doseq_unicode(self):
        self.assertEqual(urlencode([(u'\xf1', [u'\xe1', u'\xe9', 'i'])], True),
                         '%C3%B1=%C3%A1&%C3%B1=%C3%A9&%C3%B1=i')

    def test_to_str(self):
        self.assertEqual(to_str([]), '[]')
        self.assertEqual(to_str(1), '1')

    def test_to_str_unicode(self):
        self.assertEqual(to_str(u'\xf1'), '\xc3\xb1')
        self.assertEqual(to_str(u'fuho', 'rot13'), 'shub')



if __name__ == '__main__':
    unittest.main()
