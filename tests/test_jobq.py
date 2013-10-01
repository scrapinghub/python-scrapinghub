"""
Test JobQ
"""
import os, unittest
from hstestcase import HSTestCase
from hubstorage.jobq import DuplicateJobError
from hubstorage.utils import apipoll


EXCLUSIVE = os.environ.get('EXCLUSIVE_STORAGE')


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

    def test_startjob_with_extras(self):
        jobq = self.project.jobq
        pushextras = {
            'string': 'foo',
            'integer': 1,
            'float': 3.2,
            'mixedarray': ['b', 1, None, True, False, {'k': 'c'}],
            'emptyarray': [],
            'mapping': {'alpha': 5, 'b': 'B', 'cama': []},
            'emptymapping': {},
            'true': True,
            'false': False,
            'nil': None,
        }
        qj = jobq.push(self.spidername, **pushextras)
        startextras = dict(('s_' + k, v) for k, v in pushextras.iteritems())
        nj = jobq.start(**startextras)
        for k, v in dict(pushextras, **startextras).iteritems():
            if type(v) is float:
                self.assertAlmostEqual(nj.get(k), v)
            else:
                self.assertEqual(nj.get(k), v)

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

    def test_summary_jobmeta(self):
        jobq = self.project.jobq
        jobq.push(self.spidername, foo='bar', caz='fuu')
        pendings = jobq.summary('pending', jobmeta='foo')['summary']
        p1 = pendings[0]
        self.assertEqual(p1.get('foo'), 'bar')
        self.assertFalse('caz' in p1)

        pendings = jobq.summary('pending', jobmeta=['foo', 'caz'])['summary']
        p1 = pendings[0]
        self.assertEqual(p1.get('foo'), 'bar')
        self.assertEqual(p1.get('caz'), 'fuu')

    def test_summary_countstart(self):
        # push more than 5 jobs into same queue
        N = 20
        jobq = self.project.jobq
        for state in ('pending', 'running', 'finished'):
            for idx in xrange(N):
                jobq.push(self.spidername, state=state, idx=idx)

            s1 = jobq.summary(state)
            self.assertEqual(s1['count'], N)
            self.assertEqual(len(s1['summary']), 5)

            s2 = jobq.summary(state, count=N)
            self.assertEqual(len(s2['summary']), N)

            s3 = jobq.summary(state, start=N - 6, count=3)
            self.assertEqual([o['key'] for o in s3['summary']],
                             [o['key'] for o in s2['summary'][-6:-3]])

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

    def test_simple_botgroups(self):
        self.project.settings['botgroups'] = ['g1', 'g2']
        self.project.settings.save()
        pq = self.project.jobq
        hq = self.hsclient.jobq
        q1 = pq.push(self.spidername)
        q2 = pq.push(self.spidername)
        self.assertEqual(hq.start(botgroup='g3'), None)
        self.assertEqual(apipoll(hq.start, botgroup='g1')['key'], q1['key'])
        self.assertEqual(apipoll(hq.start, botgroup='g2')['key'], q2['key'])

    @unittest.skipUnless(EXCLUSIVE, "test requires exclusive"
        " (without any active bots) access to HS. Set EXCLUSIVE_STORAGE"
        " env. var to activate")
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
        self.assertEqual(apipoll(hq.start, botgroup='g1')['key'], q1['key'])
        self.assertEqual(apipoll(hq.start, botgroup='g2')['key'], q2['key'])

        # cleanup project botgroups, q3 must be polled only by generic bots
        del self.project.settings['botgroups']
        self.project.settings.save()
        q4 = pq.push(self.spidername)
        self.assertEqual(hq.start(botgroup='g1'), None)
        self.assertEqual(hq.start(botgroup='g2'), None)
        self.assertEqual(hq.start(botgroup='g3'), None)
        self.assertEqual(hq.start()['key'], q3['key'])
        self.assertEqual(hq.start()['key'], q4['key'])

        self.project.settings['botgroups'] = ['python-hubstorage-test']
        self.project.settings.save()

    def test_spider_updates(self):
        jobq = self.project.jobq
        spiderkey = '%s/%s' % (self.projectid, self.spiderid)
        q1 = jobq.push(self.spidername)
        q2 = jobq.push(self.spidername, state='running')
        q3 = jobq.push(self.spidername, state='finished')
        q4 = jobq.push(self.spidername, state='deleted')
        r = dict((x['key'], x['prevstate']) for x in jobq.delete(spiderkey))
        self.assertEqual(r.get(q1['key']), 'pending', r)
        self.assertEqual(r.get(q2['key']), 'running', r)
        self.assertEqual(r.get(q3['key']), 'finished', r)
        self.assertTrue(q4['key'] not in r)

        # Empty result set
        self.assertFalse(list(jobq.delete(spiderkey)))

    def test_multiple_job_update(self):
        jobq = self.project.jobq
        q1 = jobq.push(self.spidername)
        q2 = jobq.push(self.spidername)
        q3 = jobq.push(self.spidername)
        ids = [q1, q2['key'], self.project.get_job(q3['key'])]
        self.assertTrue([x['prevstate'] for x in jobq.start(ids)],
                        ['pending', 'pending', 'pending'])
        self.assertTrue([x['prevstate'] for x in jobq.finish(ids)],
                        ['running', 'running', 'running'])
        self.assertTrue([x['prevstate'] for x in jobq.delete(ids)],
                        ['finished', 'finished', 'finished'])
