"""
Test JobQ
"""
from hstestcase import HSTestCase


class ActivityTest(HSTestCase):

    def setUp(self):
        super(ActivityTest, self).setUp()
        self.jobq = self.hsclient.get_jobq(self.projectid)

    def test_basic(self):
        #authpos(JOBQ_PUSH_URL, data="", expect=400)
        spider1 = self.jobq.push('spidey')
        spider2 = self.jobq.push(spider='spidey')
        spider3 = self.jobq.push(spider='spidey', metatest='somekey')
        spider4 = self.jobq.push('spidey')
        summary = dict((s['name'], s) for s in self.jobq.summary())
        pending = summary['pending']
        pending_summaries = pending['summary']
        assert len(pending_summaries) >= 4
        assert len(pending_summaries) <= 8 # 8 are requested
        assert pending['count'] >= len(pending_summaries)

        # expected keys, in the order they should be in the queue
        expected_keys = [spider4['key'], spider3['key'], spider2['key'], spider1['key']]
        # only count the keys we inserted, as other tests may be running
        def filter_test(summary):
            """filter out all summaries not in our test"""
            return [s['key'] for s in summary if s['key'] in expected_keys]

        received_keys = filter_test(pending_summaries)
        assert expected_keys == received_keys

        # change some job states
        job1 = self.hsclient.get_job(spider1['key'])
        job1.finished()
        job2 = self.hsclient.get_job(spider2['key'])
        job2.started()

        # check job queues again
        summary = dict((s['name'], s) for s in self.jobq.summary())
        assert summary['pending']['count'] >= 2
        assert summary['running']['count'] >= 1
        assert summary['finished']['count'] >= 1

        pending_keys = filter_test(summary['pending']['summary'])
        assert pending_keys == [spider4['key'], spider3['key']]
        running_keys = filter_test(summary['running']['summary'])
        assert running_keys == [spider2['key']]
        finished_keys = filter_test(summary['finished']['summary'])
        assert finished_keys == [spider1['key']]

        job2.finished()
        summary = dict((s['name'], s) for s in self.jobq.summary())
        finished_keys = filter_test(summary['finished']['summary'])
        assert finished_keys == [spider2['key'], spider1['key']]
