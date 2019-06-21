"""
Test JobQ
"""
import os
import six
import pytest
from six.moves import range

from scrapinghub.hubstorage.jobq import DuplicateJobError
from scrapinghub.hubstorage.utils import apipoll

from ..conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .conftest import hsspiderid


def _keys(lst):
    return [x['key'] for x in lst]


def test_push(hsclient, hsproject):
    jobq = hsproject.jobq
    qjob = jobq.push(TEST_SPIDER_NAME)
    assert 'key' in qjob, qjob
    assert 'auth' in qjob, qjob

    job = hsclient.get_job(qjob['key'])
    assert job.metadata.get('state') == u'pending'
    assert job.metadata.get('spider') == TEST_SPIDER_NAME
    assert job.metadata.get('auth') == qjob['auth']

    jobq.start(job)
    job.metadata.expire()
    assert job.metadata.get('state') == u'running'

    jobq.finish(job)
    job.metadata.expire()
    assert job.metadata.get('state') == u'finished'

    jobq.delete(job)
    job.metadata.expire()
    assert job.metadata.get('state') == u'deleted'


def test_push_with_extras(hsclient, hsproject):
    qjob = hsproject.jobq.push(TEST_SPIDER_NAME, foo='bar', baz='fuu')
    job = hsclient.get_job(qjob['key'])
    assert job.metadata.get('foo') == u'bar'
    assert job.metadata.get('baz') == u'fuu'


def test_push_with_priority(hsclient, hsproject):
    jobq = hsproject.jobq
    qjob = jobq.push(TEST_SPIDER_NAME, priority=jobq.PRIO_HIGHEST)
    assert 'key' in qjob, qjob
    assert 'auth' in qjob, qjob


def test_push_with_state(hsclient, hsproject):
    qjob = hsproject.jobq.push(TEST_SPIDER_NAME, state='running')
    assert 'key' in qjob, qjob
    assert 'auth' in qjob, qjob
    job = hsclient.get_job(qjob['key'])
    assert job.metadata.get('state') == u'running'


def test_push_with_unique(hsproject):
    jobq = hsproject.jobq
    # no unique key
    jobq.push(TEST_SPIDER_NAME)
    jobq.push(TEST_SPIDER_NAME)
    jobq.push(TEST_SPIDER_NAME, unique=None)
    jobq.push(TEST_SPIDER_NAME, unique=None)

    # unique key
    q1 = jobq.push(TEST_SPIDER_NAME, unique='h1')
    jobq.push(TEST_SPIDER_NAME, unique='h2')
    with pytest.raises(DuplicateJobError):
        jobq.push(TEST_SPIDER_NAME, unique='h1')
    jobq.finish(q1)
    with pytest.raises(DuplicateJobError):
        jobq.push(TEST_SPIDER_NAME, unique='h2')
    jobq.push(TEST_SPIDER_NAME, unique='h1')


def test_startjob(hsproject):
    jobq = hsproject.jobq
    qj = jobq.push(TEST_SPIDER_NAME)
    nj = jobq.start()
    assert nj.pop('pending_time', None), nj
    assert nj.pop('running_time', None), nj
    assert nj.pop('auth', None), nj
    assert nj[u'key'] == qj['key']
    assert nj[u'spider'] == TEST_SPIDER_NAME
    assert nj[u'state'] == u'running'
    assert nj[u'priority'] == jobq.PRIO_NORMAL


def test_startjob_with_extras(hsproject):
    jobq = hsproject.jobq
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
    qj = jobq.push(TEST_SPIDER_NAME, **pushextras)
    startextras = dict(('s_' + k, v) for k, v in six.iteritems(pushextras))
    nj = jobq.start(**startextras)
    assert qj['key'] == nj['key']
    for k, v in six.iteritems(dict(pushextras, **startextras)):
        if type(v) is float:
            assert abs(nj.get(k) - v) < 0.0001
        else:
            assert nj.get(k) == v


def test_startjob_order(hsproject):
    jobq = hsproject.jobq
    q1 = jobq.push(TEST_SPIDER_NAME)
    q2 = jobq.push(TEST_SPIDER_NAME)
    q3 = jobq.push(TEST_SPIDER_NAME)
    assert jobq.start()['key'] == q1['key']
    assert jobq.start()['key'] == q2['key']
    assert jobq.start()['key'] == q3['key']


def test_summary(hsproject):
    jobq = hsproject.jobq
    # push at least one job per state
    jobq.push(TEST_SPIDER_NAME)
    jobq.push(TEST_SPIDER_NAME, state='running')
    jobq.push(TEST_SPIDER_NAME, state='finished')
    summaries = dict((s['name'], s) for s in jobq.summary())
    assert set(summaries), set(['pending', 'running', 'finished'])
    assert jobq.summary('pending')
    assert jobq.summary('running')
    assert jobq.summary('finished')


def test_summary_jobmeta(hsproject):
    jobq = hsproject.jobq
    jobq.push(TEST_SPIDER_NAME, foo='bar', caz='fuu')
    pendings = jobq.summary('pending', jobmeta='foo')['summary']
    p1 = pendings[0]
    assert p1.get('foo') == 'bar'
    assert 'caz' not in p1

    pendings = jobq.summary('pending', jobmeta=['foo', 'caz'])['summary']
    p1 = pendings[0]
    assert p1.get('foo') == 'bar'
    assert p1.get('caz') == 'fuu'


def test_summary_countstart(hsproject):
    # push more than 5 jobs into same queue
    N = 6
    jobq = hsproject.jobq
    for state in ('pending', 'running', 'finished'):
        for idx in range(N):
            jobq.push(TEST_SPIDER_NAME, state=state, idx=idx)

        s1 = jobq.summary(state)
        assert s1['count'] == N
        assert len(s1['summary']) == 5

        s2 = jobq.summary(state, count=N)
        assert len(s2['summary']) == N

        s3 = jobq.summary(state, start=N - 6, count=3)
        assert ([o['key'] for o in s3['summary']] ==
                [o['key'] for o in s2['summary'][-6:-3]])


def test_summaries_and_state_changes(hsproject, hsspiderid):
    jobq = hsproject.jobq
    j1 = jobq.push(TEST_SPIDER_NAME)
    j2 = jobq.push(TEST_SPIDER_NAME)
    j3 = jobq.push(TEST_SPIDER_NAME)
    j4 = jobq.push(TEST_SPIDER_NAME, state='running')
    # check queue summaries
    _assert_queue(hsproject, hsspiderid, 'pending', [j3, j2, j1])
    _assert_queue(hsproject, hsspiderid, 'running', [j4])
    _assert_queue(hsproject, hsspiderid, 'finished', [])
    # change job states
    jobq.start(j1)
    jobq.finish(j2)
    jobq.finish(j4)
    # check summaries again
    _assert_queue(hsproject, hsspiderid, 'pending', [j3])
    _assert_queue(hsproject, hsspiderid, 'running', [j1])
    _assert_queue(hsproject, hsspiderid, 'finished', [j4, j2])
    # delete all jobs and check for empty summaries
    jobq.finish(j1)
    jobq.finish(j3)
    jobq.delete(j1)
    jobq.delete(j2)
    jobq.delete(j3)
    jobq.delete(j4)
    _assert_queue(hsproject, hsspiderid, 'pending', [])
    _assert_queue(hsproject, hsspiderid, 'running', [])
    _assert_queue(hsproject, hsspiderid, 'finished', [])


def test_list_with_state(hsproject):
    jobq = hsproject.jobq
    j1 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j2 = jobq.push(TEST_SPIDER_NAME, state='running')
    j3 = jobq.push(TEST_SPIDER_NAME, state='pending')
    j4 = jobq.push(TEST_SPIDER_NAME, state='finished')
    # Only finished jobs by default
    assert _keys(jobq.list()) == _keys([j4, j1])
    assert _keys(jobq.list(state='finished')) == _keys([j4, j1])
    assert _keys(jobq.list(state='running')) == _keys([j2])
    assert _keys(jobq.list(state=['running', 'pending'])) == _keys([j3, j2])


def test_list_with_count(hsproject):
    jobq = hsproject.jobq
    j1 = jobq.push(TEST_SPIDER_NAME, state='finished')  # NOQA
    j2 = jobq.push(TEST_SPIDER_NAME, state='finished')  # NOQA
    j3 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j4 = jobq.push(TEST_SPIDER_NAME, state='finished')
    # fetch only the 2 most recent jobs
    assert _keys(jobq.list(count=2)) == _keys([j4, j3])


def test_list_with_stop(hsproject):
    jobq = hsproject.jobq
    j1 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j2 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j3 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j4 = jobq.push(TEST_SPIDER_NAME, state='finished')
    # test "stop" parameter
    # we should stop before the 4th finished job
    assert _keys(jobq.list(stop=j1['key'])) == _keys([j4, j3, j2])


def test_list_with_tags(hsproject):
    jobq = hsproject.jobq
    j1 = jobq.push(TEST_SPIDER_NAME, state='finished', tags=['t1'])
    j2 = jobq.push(TEST_SPIDER_NAME, state='finished', tags=['t2'])
    j3 = jobq.push(TEST_SPIDER_NAME, state='finished', tags=['t1', 't2'])
    j4 = jobq.push(TEST_SPIDER_NAME, state='finished')
    assert _keys(jobq.list(has_tag='t1')) == _keys([j3, j1])
    assert _keys(jobq.list(has_tag=['t2', 't1'])) == _keys([j3, j2, j1])
    assert _keys(jobq.list(has_tag='t2', lacks_tag='t1')) == _keys([j2])
    assert _keys(jobq.list(lacks_tag=['t1', 't2'])) == _keys([j4])


# endts is not implemented
@pytest.mark.xfail
def test_list_with_startts_endts(hsproject):
    jobq = hsproject.jobq
    j1 = jobq.push(TEST_SPIDER_NAME, state='finished')  # NOQA
    j2 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j3 = jobq.push(TEST_SPIDER_NAME, state='finished')
    j4 = jobq.push(TEST_SPIDER_NAME, state='finished')  # NOQA
    # test "startts/endts" parameters
    # endts is not inclusive
    # so we should get the 2 in the middle out of 4
    timestamps = [j['ts'] for j in jobq.list()]
    jobs = jobq.list(startts=timestamps[2], endts=timestamps[3])
    assert _keys(jobs) == _keys([j3, j2])


def test_spider_updates(hsproject, hsspiderid):
    jobq = hsproject.jobq
    spiderkey = '%s/%s' % (TEST_PROJECT_ID, hsspiderid)

    def finish_and_delete_jobs():
        for job in jobq.finish(spiderkey):
            yield job
        jobq.delete(spiderkey)

    q1 = jobq.push(TEST_SPIDER_NAME)
    q2 = jobq.push(TEST_SPIDER_NAME, state='running')
    q3 = jobq.push(TEST_SPIDER_NAME, state='finished')
    q4 = jobq.push(TEST_SPIDER_NAME, state='deleted')

    r = dict((x['key'], x['prevstate']) for x in finish_and_delete_jobs())
    assert r.get(q1['key']) == 'pending', r
    assert r.get(q2['key']) == 'running', r
    assert r.get(q3['key']) == 'finished', r
    assert q4['key'] not in r

    # Empty result set
    assert not list(jobq.delete(spiderkey))


def test_multiple_job_update(hsproject):
    jobq = hsproject.jobq
    q1 = jobq.push(TEST_SPIDER_NAME)
    q2 = jobq.push(TEST_SPIDER_NAME)
    q3 = jobq.push(TEST_SPIDER_NAME)
    ids = [q1, q2['key'], hsproject.get_job(q3['key'])]
    assert ([x['prevstate'] for x in jobq.start(ids)] ==
            ['pending', 'pending', 'pending'])
    assert ([x['prevstate'] for x in jobq.finish(ids)] ==
            ['running', 'running', 'running'])
    assert ([x['prevstate'] for x in jobq.delete(ids)] ==
            ['finished', 'finished', 'finished'])


def test_update(hsproject):
    job = hsproject.push_job(TEST_SPIDER_NAME)
    assert job.metadata['state'] == 'pending'
    hsproject.jobq.update(job, state='running', foo='bar')
    job = hsproject.get_job(job.key)
    assert job.metadata['state'] == 'running'
    assert job.metadata['foo'] == 'bar'


def test_jobsummary(hsproject):
    jobs = [hsproject.push_job(TEST_SPIDER_NAME, foo=i)
            for i in range(5)]
    jobmetas = list(hsproject.jobq.jobsummary(
        jobkeys=[j.key for j in jobs], jobmeta=['key', 'foo']))
    jobmeta_dict = {jm['key']: jm['foo'] for jm in jobmetas}
    assert jobmeta_dict == {
        jobs[i].key: i
        for i in range(5)
    }


def _assert_queue(hsproject, hsspiderid, qname, jobs):
    summary = hsproject.jobq.summary(qname, spiderid=hsspiderid)
    assert summary['name'] == qname
    assert summary['count'] == len(jobs)
    assert len(summary['summary']) == len(jobs)
    # Most recent jobs first
    assert ([s['key'] for s in summary['summary']] ==
            [j['key'] for j in jobs])
