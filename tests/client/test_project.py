import types
from collections import defaultdict

import pytest
from six.moves import range

from scrapinghub.client import Jobs, Job
from scrapinghub.exceptions import DuplicateJobError
from scrapinghub.client import Activity, Collections, Spiders
from scrapinghub.client import Frontier, Settings

from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .utils import validate_default_meta


def test_project_subresources(project):
    assert project.id == int(TEST_PROJECT_ID)
    assert isinstance(project.collections, Collections)
    assert isinstance(project.jobs, Jobs)
    assert isinstance(project.spiders, Spiders)
    assert isinstance(project.activity, Activity)
    assert isinstance(project.frontier, Frontier)
    assert isinstance(project.settings, Settings)


def test_project_jobs(project):
    jobs = project.jobs
    assert jobs.projectid == int(TEST_PROJECT_ID)
    assert jobs.spider is None


def test_project_jobs_count(project):
    assert project.jobs.count() == 0
    assert project.jobs.count(state=['pending', 'running', 'finished']) == 0

    project.jobs.schedule(TEST_SPIDER_NAME)
    assert project.jobs.count(state='pending') == 1

    for i in range(2):
        project.jobs.schedule(TEST_SPIDER_NAME,
                              subid='running-%s' % i,
                              meta={'state': 'running'})
    assert project.jobs.count(state='running') == 2

    for i in range(3):
        project.jobs.schedule(TEST_SPIDER_NAME,
                              subid='finished%s' % i,
                              meta={'state': 'finished'})
    assert project.jobs.count(state='finished') == 3

    assert project.jobs.count(state=['pending', 'running', 'finished']) == 6
    assert project.jobs.count(state='pending') == 1
    assert project.jobs.count(state='running') == 2
    assert project.jobs.count(state='finished') == 3
    assert project.jobs.count() == 3


def test_project_jobs_iter(project):
    project.jobs.schedule(TEST_SPIDER_NAME, meta={'state': 'running'})

    # no finished jobs
    jobs0 = project.jobs.iter()
    assert isinstance(jobs0, types.GeneratorType)
    with pytest.raises(StopIteration):
        next(jobs0)

    # filter by state must work
    jobs1 = project.jobs.iter(state='running')
    job = next(jobs1)
    assert isinstance(job, dict)
    ts = job.get('ts')
    assert isinstance(ts, int) and ts > 0
    running_time = job.get('running_time')
    assert isinstance(running_time, int) and running_time > 0
    elapsed = job.get('elapsed')
    assert isinstance(elapsed, int) and elapsed > 0
    assert job.get('key').startswith(TEST_PROJECT_ID)
    assert job.get('spider') == TEST_SPIDER_NAME
    assert job.get('state') == 'running'

    with pytest.raises(StopIteration):
        next(jobs1)


def test_project_jobs_schedule(project):
    # scheduling on project level requires spidername
    with pytest.raises(ValueError):
        project.jobs.schedule()

    job0 = project.jobs.schedule(TEST_SPIDER_NAME)
    assert isinstance(job0, Job)
    validate_default_meta(job0.metadata, state='pending')
    assert isinstance(job0.metadata.get('pending_time'), int)
    assert job0.metadata['pending_time'] > 0
    assert job0.metadata.get('scheduled_by')

    # running the same spider with same args leads to duplicate error
    with pytest.raises(DuplicateJobError):
        project.jobs.schedule(TEST_SPIDER_NAME)

    job1 = project.jobs.schedule(TEST_SPIDER_NAME,
                                 arg1='val1', arg2='val2',
                                 priority=3, units=3,
                                 add_tag=['tagA', 'tagB'],
                                 meta={'state': 'running', 'meta1': 'val1'})
    assert isinstance(job1, Job)
    meta = job1.metadata
    validate_default_meta(meta, state='running', units=3, priority=3,
                          tags=['tagA', 'tagB'])
    assert meta.get('meta1') == 'val1'
    assert meta.get('spider_args') == {'arg1': 'val1', 'arg2': 'val2'}
    assert isinstance(meta.get('running_time'), int)
    assert meta['running_time'] > 0
    assert meta.get('started_by')


def test_project_jobs_get(project):
    # error when using different project id in jobkey
    with pytest.raises(ValueError):
        project.jobs.get('1/2/3')

    fake_job = project.jobs.get(TEST_PROJECT_ID + '/2/3')
    assert isinstance(fake_job, Job)


def test_project_jobs_summary(project):
    expected_summary = [{'count': 0, 'name': state, 'summary': []}
                        for state in ['pending', 'running', 'finished']]
    assert project.jobs.summary() == expected_summary

    counts = {'pending': 1, 'running': 2, 'finished': 3}
    jobs = defaultdict(list)
    for state in sorted(counts):
        for i in range(counts[state]):
            job = project.jobs.schedule(TEST_SPIDER_NAME,
                                        subid=state + str(i),
                                        meta={'state': state})
            jobs[state].append(job.key)
    summary1 = project.jobs.summary()
    for summ in summary1:
        summ_name, summ_data = summ['name'], summ['summary']
        assert summ['count'] == counts[summ_name]
        assert isinstance(summ_data, list)
        assert len(summ_data) == counts[summ_name]
        summ_jobkeys = sorted([d['key'] for d in summ_data])
        assert summ_jobkeys == sorted(jobs[summ_name])

    # filter by queuename
    summary2 = project.jobs.summary('running')
    assert summary2['count'] == counts['running']

    # limit by count
    summary3 = project.jobs.summary('finished', count=1)
    assert summary3['count'] == counts['finished']
    assert len(summary3['summary']) == 1

    # additional jobmeta in response
    summary4 = project.jobs.summary('finished', jobmeta=['units'])
    assert summary4['count'] == counts['finished']
    assert len(summary4['summary']) == 3
    assert summary4['summary'][0].get('units') == 1


def test_project_jobs_iter_last(project):
    lastsumm0 = project.jobs.iter_last()
    assert isinstance(lastsumm0, types.GeneratorType)
    assert list(lastsumm0) == []

    job1 = project.jobs.schedule(TEST_SPIDER_NAME, meta={'state': 'finished'})
    lastsumm1 = list(project.jobs.iter_last())
    assert len(lastsumm1) == 1
    assert lastsumm1[0].get('key') == job1.key
    assert lastsumm1[0].get('spider') == TEST_SPIDER_NAME
    assert lastsumm1[0].get('state') == 'finished'
    assert lastsumm1[0].get('elapsed') > 0
    assert lastsumm1[0].get('finished_time') > 0
    assert lastsumm1[0].get('ts') > 0

    # next iter_last should return last spider's job
    job2 = project.jobs.schedule(TEST_SPIDER_NAME,
                                 subid=1,
                                 meta={'state': 'finished'})
    lastsumm2 = list(project.jobs.iter_last())
    assert len(lastsumm2) == 1
    assert lastsumm2[0].get('key') == job2.key