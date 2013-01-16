"""
Test JobQ
"""
from hstestcase import HSTestCase


class JobqTest(HSTestCase):

    def test_push(self):
        jobq = self.project.jobq
        qjob = jobq.push(self.spidername)
        self.assertTrue('key' in qjob, qjob)
        self.assertTrue('auth' in qjob, qjob)

        job = self.hsclient.get_job(qjob['key'])
        self.assertEqual(job.metadata.get('state'), u'pending')
        self.assertEqual(job.metadata.get('spider'), self.spidername)
        self.assertEqual(job.metadata.get('auth'), qjob['auth'])

        jobq.start(job)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('state'), u'running')

        jobq.finish(job)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('state'), u'finished')

        jobq.delete(job)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('state'), u'deleted')

    def test_push_with_extras(self):
        qjob = self.project.jobq.push(self.spidername, foo='bar', baz='fuu')
        job = self.hsclient.get_job(qjob['key'])
        self.assertEqual(job.metadata.get('foo'), u'bar')
        self.assertEqual(job.metadata.get('baz'), u'fuu')

    def test_push_with_priority(self):
        jobq = self.project.jobq
        qjob = jobq.push(self.spidername, priority=20)
        self.assertTrue('key' in qjob, qjob)
        self.assertTrue('auth' in qjob, qjob)

    def test_push_with_state(self):
        qjob = self.project.jobq.push(self.spidername, state='running')
        self.assertTrue('key' in qjob, qjob)
        self.assertTrue('auth' in qjob, qjob)
        job = self.hsclient.get_job(qjob['key'])
        self.assertEqual(job.metadata.get('state'), u'running')

    def test_project_new_job(self):
        job = self.project.new_job(self.spidername, state='running',
                                   priority=10, foo=u'bar')
        self.assertEqual(job.metadata.get('state'), u'running')
        self.assertEqual(job.metadata.get('foo'), u'bar')
        self.project.jobq.delete(job)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('state'), u'deleted')
        self.assertEqual(job.metadata.get('foo'), u'bar')

    def test_client_new_job(self):
        job = self.hsclient.new_job(self.projectid, self.spidername,
                                    state='running', priority=5, foo='baz')
        self.assertEqual(job.metadata.get('state'), u'running', self.hsclient.auth)
        self.assertEqual(job.metadata.get('foo'), u'baz')
        self.project.jobq.delete(job)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('state'), u'deleted')
        self.assertEqual(job.metadata.get('foo'), u'baz')

    def test_project_summary(self):
        jobq = self.project.jobq
        # push at least one job per state
        jobq.push(self.spidername)
        jobq.push(self.spidername, state='running')
        jobq.push(self.spidername, state='finished')
        summaries = dict((s['name'], s) for s in jobq.summary())
        self.assertEqual(set(summaries), set(['pending', 'running', 'finished']))
        self.assertTrue(jobq.summary('pending'))
        self.assertTrue(jobq.summary('running'))
        self.assertTrue(jobq.summary('finished'))

    def test_summaries_and_state_changes(self):
        jobq = self.project.jobq
        j1 = jobq.push(self.spidername)
        j2 = jobq.push(self.spidername)
        j3 = jobq.push(self.spidername)
        j4 = jobq.push(self.spidername, state='running')
        # check queue summaries
        self._assert_queue('pending', [j3, j2, j1])
        self._assert_queue('running', [j4])
        self._assert_queue('finished', [])
        # change job states
        jobq.start(j1)
        jobq.finish(j2)
        jobq.finish(j4)
        # check summaries again
        self._assert_queue('pending', [j3])
        self._assert_queue('running', [j1])
        self._assert_queue('finished', [j4, j2])
        # delete all jobs and check for empty summaries
        jobq.delete(j1)
        jobq.delete(j2)
        jobq.delete(j3)
        jobq.delete(j4)
        self._assert_queue('pending', [])
        self._assert_queue('running', [])
        self._assert_queue('finished', [])

    def _assert_queue(self, qname, jobs):
        summary = self.project.jobq.summary(qname, spiderid=self.spiderid)
        # FIXME: when a queue have not items HS does not return its summary
        if summary is None:
            self.assertEqual(len(jobs), 0)
            return

        self.assertEqual(summary['name'], qname)
        # FIXME: HS returns the total count and not the spider count for the queue
        #self.assertEqual(summary['count'], len(jobs))
        self.assertEqual(len(summary['summary']), len(jobs))
        # Most recent jobs first
        self.assertEqual([s['key'] for s in summary['summary']],
                         [j['key'] for j in jobs])
