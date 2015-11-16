"""
Test Client
"""
from hstestcase import HSTestCase
from hubstorage.utils import millitime, apipoll

class ClientTest(HSTestCase):

    def test_push_job(self):
        c = self.hsclient
        c.push_job(self.projectid, self.spidername,
                   priority=self.project.jobq.PRIO_LOW,
                   foo='baz')
        job = c.start_job(projectid=self.projectid)
        m = job.metadata
        self.assertEqual(m.get('state'), u'running', c.auth)
        self.assertEqual(m.get('foo'), u'baz')
        self.project.jobq.delete(job)

        # job auth token is valid only while job is running
        m = c.get_job(job.key).metadata
        self.assertEqual(m.get('state'), u'deleted')
        self.assertEqual(m.get('foo'), u'baz')

    def test_botgroup(self):
        self.project.settings.update(botgroups=['foo'], created=millitime())
        self.project.settings.save()
        c = self.hsclient
        q1 = c.push_job(self.project.projectid, self.spidername)
        j1 = c.start_job()
        self.assertEqual(j1, None, 'got %s, pushed job was %s' % (j1, q1))
        j2 = c.start_job(botgroup='bar')
        self.assertEqual(j2, None, 'got %s, pushed job was %s' % (j2, q1))
        j3 = apipoll(self.hsclient.start_job, botgroup='foo')
        self.assertEqual(j3.key, q1.key)

    def test_start_job(self):
        # Pending queue is empty
        job = self.hsclient.start_job(botgroup=self.testbotgroup)
        self.assertEqual(job, None)
        # Push a new job into pending queue
        j0 = self.hsclient.push_job(self.projectid, self.spidername)
        # Assert it is pending
        self.assertEqual(j0.metadata.get('state'), 'pending')
        # Poll for the pending job
        j1 = apipoll(self.hsclient.start_job, botgroup=self.testbotgroup)
        # Assert started job does not need an extra request to fetch its metadata
        self.assertTrue(j1.metadata._cached is not None)
        # Assert started job is in running queue
        self.assertEqual(j1.metadata.get('state'), 'running')
        # Started job metadata must match metadata retrieved directly from /jobs/
        j2 = self.hsclient.get_job(j1.key)
        self.assertEqual(dict(j1.metadata), dict(j2.metadata))

    def test_jobsummaries(self):
        hsc = self.hsclient
        # add at least one running or pending job to ensure summary is returned
        hsc.push_job(self.projectid, self.spidername, state='running')

        def _get_summary():
            jss = hsc.projects.jobsummaries()
            mjss = dict((str(js['project']), js) for js in jss)
            return mjss.get(self.projectid)
        summary = apipoll(_get_summary)
        self.assertIsNotNone(summary)

    def test_timestamp(self):
        ts1 = self.hsclient.server_timestamp()
        ts2 = self.hsclient.server_timestamp()
        self.assertGreater(ts1, 0)
        self.assertLessEqual(ts1, ts2)
