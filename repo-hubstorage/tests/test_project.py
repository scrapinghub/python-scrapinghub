"""
Test Project
"""
import json
import six
from six.moves import range
from random import randint, random
from requests.exceptions import HTTPError
from hubstorage import HubstorageClient
from hubstorage.utils import millitime
from .hstestcase import HSTestCase
from .testutil import failing_downloader


class ProjectTest(HSTestCase):

    def test_projectid(self):
        p1 = self.hsclient.get_project(int(self.projectid))
        p2 = self.hsclient.get_project(str(self.projectid))
        self.assertEqual(p1.projectid, p2.projectid)
        self.assertEqual(type(p1.projectid), six.text_type)
        self.assertEqual(type(p2.projectid), six.text_type)
        self.assertRaises(AssertionError, self.hsclient.get_project, '111/3')

    def test_get_job_from_key(self):
        job = self.project.push_job(self.spidername)
        parts = tuple(job.key.split('/'))
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[:2], (self.projectid, self.spiderid))
        samejob1 = self.hsclient.get_job(job.key)
        samejob2 = self.project.get_job(job.key)
        samejob3 = self.project.get_job(parts[1:])
        self.assertEqual(samejob1.key, job.key)
        self.assertEqual(samejob2.key, job.key)
        self.assertEqual(samejob3.key, job.key)

    def test_get_jobs(self):
        p = self.project
        j1 = p.push_job(self.spidername, testid=0)
        j2 = p.push_job(self.spidername, testid=1)
        j3 = p.push_job(self.spidername, testid=2)
        # global list must list at least one job
        self.assertTrue(list(p.get_jobs(count=1, state='pending')))
        # List all jobs for test spider
        r = list(p.get_jobs(spider=self.spidername, state='pending'))
        self.assertEqual([j.key for j in r], [j3.key, j2.key, j1.key])

    def test_get_jobs_with_legacy_filter(self):
        p = self.project
        j1 = p.push_job(self.spidername, state='finished',
                        close_reason='finished', tags=['t2'])
        j2 = p.push_job(self.spidername, state='finished',
                        close_reason='finished', tags=['t1'])
        j3 = p.push_job(self.spidername, state='pending')
        j4 = p.push_job(self.spidername, state='finished',
                        close_reason='failed', tags=['t1'])
        j5 = p.push_job(self.spidername + 'skip', state='finished',
                        close_reason='failed', tags=['t1'])

        filters = [['spider', '=', [self.spidername]],
                   ['state', '=', ['finished']],
                   ['close_reason', '=', ['finished']],
                   ['tags', 'haselement', ['t1']],
                   ['tags', 'hasnotelement', ['t2']]]
        jobs = p.get_jobs(filter=[json.dumps(x) for x in filters])
        assert [j.key for j in jobs] == [j2.key], jobs

    def test_push_job(self):
        job = self.project.push_job(self.spidername, state='running',
                                   priority=self.project.jobq.PRIO_HIGH,
                                   foo=u'bar')
        self.assertEqual(job.metadata.get('state'), u'running')
        self.assertEqual(job.metadata.get('foo'), u'bar')
        self.project.jobq.finish(job)
        self.project.jobq.delete(job)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('state'), u'deleted')
        self.assertEqual(job.metadata.get('foo'), u'bar')

    def test_auth(self):
        # client without global auth set
        hsc = HubstorageClient(endpoint=self.hsclient.endpoint)
        self.assertEqual(hsc.auth, None)

        # check no-auth access
        try:
            hsc.push_job(self.projectid, self.spidername)
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)
        else:
            self.assertTrue(False, '401 not raised')

        try:
            hsc.get_project(self.projectid).push_job(self.spidername)
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)
        else:
            self.assertTrue(False, '401 not raised')

        try:
            hsc.get_job((self.projectid, 1, 1)).items.list()
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)
        else:
            self.assertTrue(False, '401 not raised')

        try:
            hsc.get_project(self.projectid).get_job((self.projectid, 1, 1)).items.list()
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)
        else:
            self.assertTrue(False, '401 not raised')

        # create project with auth
        auth = self.hsclient.auth
        project = hsc.get_project(self.projectid, auth)
        self.assertEqual(project.auth, auth)
        job = project.push_job(self.spidername)
        samejob = project.get_job(job.key)
        self.assertEqual(samejob.key, job.key)

    def test_broad(self):
        project = self.hsclient.get_project(self.projectid)
        # populate project with at least one job
        job = project.push_job(self.spidername)
        self.assertEqual(job.metadata.get('state'), 'pending')
        job = self.start_job()
        self.assertEqual(job.metadata.get('state'), 'running')
        job.items.write({'title': 'bar'})
        job.logs.info('nice to meet you')
        job.samples.write([1, 2, 3])
        job.close_writers()
        job.jobq.finish(job)

        # keep a jobid for get_job and unreference job
        jobid = job.key
        jobauth = job.auth
        del job

        self.assertTrue(list(project.items.list(self.spiderid, count=1)))
        self.assertTrue(list(project.logs.list(self.spiderid, count=1)))
        self.assertTrue(list(project.samples.list(self.spiderid, count=1)))

        job = project.client.get_job(jobid, jobauth=jobauth)
        job.purged()

    def test_settings(self):
        project = self.hsclient.get_project(self.projectid)
        settings = dict(project.settings)
        settings.pop('botgroups', None)  # ignore testsuite botgroups
        self.assertEqual(settings, {})
        project.settings['created'] = created = millitime()
        project.settings['botgroups'] = ['g1']
        project.settings.save()
        self.assertEqual(project.settings.liveget('created'), created)
        self.assertEqual(project.settings.liveget('botgroups'), ['g1'])
        project.settings.expire()
        self.assertEqual(dict(project.settings), {
            'created': created,
            'botgroups': ['g1'],
        })

    def test_requests(self):
        ts = millitime()
        job = self.project.push_job(self.spidername, state='running')
        # top parent
        r1 = job.requests.add(url='http://test.com/', status=200, method='GET',
                              rs=1337, duration=5, parent=None, ts=ts)
        # first child
        r2 = job.requests.add(url='http://test.com/2', status=400, method='POST',
                              rs=0, duration=1, parent=r1, ts=ts + 1)
        # another child with fingerprint set
        r3 = job.requests.add(url='http://test.com/3', status=400, method='PUT',
                              rs=0, duration=1, parent=r1, ts=ts + 2, fp='1234')

        job.requests.close()
        rr = job.requests.list()
        self.assertEqual(next(rr),
                         {u'status': 200, u'rs': 1337,
                          u'url': u'http://test.com/', u'time': ts,
                          u'duration': 5, u'method': u'GET'})
        self.assertEqual(next(rr),
                         {u'status': 400, u'parent': 0, u'rs': 0,
                          u'url': u'http://test.com/2', u'time': ts + 1,
                          u'duration': 1, u'method': u'POST'})
        self.assertEqual(next(rr),
                         {u'status': 400, u'fp': u'1234', u'parent': 0,
                          u'rs': 0, u'url': u'http://test.com/3',
                          u'time': ts + 2, u'duration': 1,
                          u'method': u'PUT'})

        self.assertRaises(StopIteration, next, rr)

    def test_samples(self):
        # no samples stored
        j1 = self.project.push_job(self.spidername, state='running')
        self.assertEqual(list(j1.samples.list()), [])
        # simple fill
        ts = millitime()
        j1.samples.write([ts, 1, 2, 3])
        j1.samples.write([ts + 1, 5, 9, 4])
        j1.samples.flush()
        o = list(j1.samples.list())
        self.assertEqual(len(o), 2)
        self.assertEqual(o[0], [ts, 1, 2, 3])
        self.assertEqual(o[1], [ts + 1, 5, 9, 4])

        # random fill
        j2 = self.project.push_job(self.spidername, state='running')
        samples = []
        ts = millitime()
        count = int(j2.samples.batch_size * (random() + randint(1, 5)))
        for _ in range(count):
            ts += randint(1, 2**16)
            row = [ts] + list(randint(0, 2**16) for _ in range(randint(0, 100)))
            samples.append(row)
            j2.samples.write(row)
        j2.samples.flush()
        o = list(j2.samples.list())
        self.assertEqual(len(o), count)
        for r1, r2 in zip(samples, o):
            self.assertEqual(r1, r2)

    def test_jobsummary(self):
        js = self.project.jobsummary()
        self.assertEqual(js.get('project'), int(self.project.projectid), js)
        self.assertEqual(js.get('has_capacity'), True, js)
        self.assertTrue('pending' in js, js)
        self.assertTrue('running' in js, js)

    def test_bulkdata(self):
        j = self.project.push_job(self.spidername, state='running')
        for i in range(20):
            j.logs.info("log line %d" % i)
            j.items.write(dict(field1="item%d" % i))
            j.requests.add("http://test.com/%d" % i,
                200, 'GET', 10, None, 10, 120)
        for resourcename in ('logs', 'items', 'requests'):
            resource = getattr(j, resourcename)
            resource.flush()

            # downloading resource, with simulated failures
            with failing_downloader(resource):
                downloaded = list(resource.iter_values())
                self.assertEqual(len(downloaded), 20)

    def test_output_string(self):
        project = self.hsclient.get_project(self.projectid)
        project.push_job(self.spidername)
        job = self.start_job()
        job.items.write({'foo': 'bar'})
        job.close_writers()
        items = self.hsclient.get_job(job.key).items.iter_json()
        self.assertEqual(type(next(items)), str)
