"""
Test Client
"""
from hstestcase import HSTestCase


class ClientTest(HSTestCase):

    def test_new_job(self):
        c = self.hsclient
        job = c.new_job(self.projectid, self.spidername,
                        state='running',
                        priority=self.project.jobq.PRIO_LOW,
                        foo='baz')
        m = job.metadata
        self.assertEqual(m.get('state'), u'running', c.auth)
        self.assertEqual(m.get('foo'), u'baz')
        self.project.jobq.delete(job)
        m.expire()
        self.assertEqual(m.get('state'), u'deleted')
        self.assertEqual(m.get('foo'), u'baz')
