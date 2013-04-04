"""
Test JobQ
"""
from hstestcase import HSTestCase
from hubstorage.jobq import DuplicateJobError


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
        qjob = jobq.push(self.spidername, priority=jobq.PRIO_HIGHEST)
        self.assertTrue('key' in qjob, qjob)
        self.assertTrue('auth' in qjob, qjob)

    def test_push_with_state(self):
        qjob = self.project.jobq.push(self.spidername, state='running')
        self.assertTrue('key' in qjob, qjob)
        self.assertTrue('auth' in qjob, qjob)
        job = self.hsclient.get_job(qjob['key'])
        self.assertEqual(job.metadata.get('state'), u'running')

    def test_push_with_unique(self):
        jobq = self.project.jobq
        # no unique key
        jobq.push(self.spidername)
        jobq.push(self.spidername)
        jobq.push(self.spidername, unique=None)
        jobq.push(self.spidername, unique=None)

        # unique key
        q1 = jobq.push(self.spidername, unique='h1')
        jobq.push(self.spidername, unique='h2')
        self.assertRaises(DuplicateJobError, jobq.push, self.spidername, unique='h1')
        jobq.finish(q1)
        self.assertRaises(DuplicateJobError, jobq.push, self.spidername, unique='h2')
        jobq.push(self.spidername, unique='h1')

    def test_startjob(self):
        jobq = self.project.jobq
        qj = jobq.push(self.spidername)
        nj = jobq.start()
        self.assertTrue(nj.pop('pending_time', None), nj)
        nj.pop('running_time', None)
        self.assertEqual(nj, {
            u'auth': qj['auth'],
            u'key': qj['key'],
            u'priority': jobq.PRIO_NORMAL,
            u'spider': self.spidername,
            u'state': u'running',
        })

    def test_startjob_order(self):
        jobq = self.project.jobq
        q1 = jobq.push(self.spidername)
        q2 = jobq.push(self.spidername)
        q3 = jobq.push(self.spidername)
        self.assertEqual(jobq.start()['key'], q1['key'])
        self.assertEqual(jobq.start()['key'], q2['key'])
        self.assertEqual(jobq.start()['key'], q3['key'])

    def test_summary(self):
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
        self.assertEqual(summary['name'], qname)
        self.assertEqual(summary['count'], len(jobs))
        self.assertEqual(len(summary['summary']), len(jobs))
        # Most recent jobs first
        self.assertEqual([s['key'] for s in summary['summary']],
                         [j['key'] for j in jobs])

    def test_botgroups(self):
        self.project.settings['botgroups'] = ['g1', 'g2']
        self.project.settings.save()
        pq = self.project.jobq
        hq = self.hsclient.jobq
        q1 = pq.push(self.spidername)
        q2 = pq.push(self.spidername)
        q3 = pq.push(self.spidername)
        self.assertEqual(hq.start(), None)
        self.assertEqual(hq.start(botgroup='g3'), None)
        self.assertEqual(hq.start(botgroup='g1')['key'], q1['key'])
        self.assertEqual(hq.start(botgroup='g2')['key'], q2['key'])

        # cleanup project botgroups, q3 must be polled only by generic bots
        del self.project.settings['botgroups']
        self.project.settings.save()
        q4 = pq.push(self.spidername)
        self.assertEqual(hq.start(botgroup='g1'), None)
        self.assertEqual(hq.start(botgroup='g2'), None)
        self.assertEqual(hq.start(botgroup='g3'), None)
        self.assertEqual(hq.start()['key'], q3['key'])
        self.assertEqual(hq.start()['key'], q4['key'])
