"""
Test Client
"""
from .hstestcase import HSTestCase
from hubstorage.utils import millitime, apipoll

class ClientTest(HSTestCase):

    def test_push_job(self):
        c = self.hsclient
        c.push_job(self.projectid, self.spidername,
                   priority=self.project.jobq.PRIO_LOW,
                   foo='baz')
        job = self.start_job()
        m = job.metadata
        self.assertEqual(m.get('state'), u'running', c.auth)
        self.assertEqual(m.get('foo'), u'baz')
        self.project.jobq.finish(job)
        self.project.jobq.delete(job)

        # job auth token is valid only while job is running
        m = c.get_job(job.key).metadata
        self.assertEqual(m.get('state'), u'deleted')
        self.assertEqual(m.get('foo'), u'baz')

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
