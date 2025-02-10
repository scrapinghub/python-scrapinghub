import types
from collections import defaultdict

import pytest

from scrapinghub.client.exceptions import DuplicateJobError
from scrapinghub.client.exceptions import BadRequest
from scrapinghub.client.exceptions import NotFound
from scrapinghub.client.jobs import Jobs, Job
from scrapinghub.client.spiders import Spider
from scrapinghub.client.utils import JobKey

from ..conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .utils import validate_default_meta


def test_spiders_get(project):
    spider = project.spiders.get(TEST_SPIDER_NAME)
    assert isinstance(spider, Spider)
    assert isinstance(spider.jobs, Jobs)

    with pytest.raises(NotFound):
        project.spiders.get('non-existing')


def test_spiders_list(project):
    assert project.spiders.list() == [
        {'id': 'hs-test-spider', 'tags': [],
         'type': 'manual', 'version': None}]


def test_spider_base(project, spider):
    assert isinstance(spider._id, str)
    assert isinstance(spider.key, str)
    assert spider.key == spider.project_id + '/' + spider._id
    assert spider.name == TEST_SPIDER_NAME
    assert spider.project_id == TEST_PROJECT_ID
    assert isinstance(project.jobs, Jobs)


def test_spider_list_update_tags(project, spider):
    with pytest.raises(BadRequest):
        spider.update_tags()

    spider.update_tags(add=['new1', 'new2'])
    assert spider.list_tags() == ['new1', 'new2']
    spider.update_tags(add=['new2', 'new3'], remove=['new1'])
    assert spider.list_tags() == ['new2', 'new3']
    spider.update_tags(remove=['new2', 'new3'])
    assert spider.list_tags() == []


def test_spider_jobs(spider):
    jobs = spider.jobs
    assert jobs.project_id == TEST_PROJECT_ID
    assert jobs.spider is spider


def test_spider_jobs_count(spider):
    jobs = spider.jobs
    assert jobs.count() == 0
    assert jobs.count(state=['pending', 'running', 'finished']) == 0

    jobs.run()
    assert jobs.count(state='pending') == 1

    for i in range(2):
        jobs.run(job_args={'subid': 'running-%s' % i},
                 meta={'state': 'running'})
    assert jobs.count(state='running') == 2

    for i in range(3):
        jobs.run(job_args={'subid': 'finished%s' % i},
                 meta={'state': 'finished'})
    assert jobs.count(state='finished') == 3

    assert jobs.count(state=['pending', 'running', 'finished']) == 6
    assert jobs.count(state='pending') == 1
    assert jobs.count(state='running') == 2
    assert jobs.count(state='finished') == 3
    assert jobs.count() == 3


def test_spider_jobs_iter(spider):
    spider.jobs.run(meta={'state': 'running'})

    # no finished jobs
    jobs0 = spider.jobs.iter()
    assert isinstance(jobs0, types.GeneratorType)
    with pytest.raises(StopIteration):
        next(jobs0)

    # filter by state must work
    jobs1 = spider.jobs.iter(state='running')
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


def test_spider_jobs_list(spider):
    spider.jobs.run(meta={'state': 'running'})

    # no finished jobs
    jobs0 = spider.jobs.list()
    assert isinstance(jobs0, list)
    assert len(jobs0) == 0

    # filter by state must work like for iter
    jobs1 = spider.jobs.list(state='running')
    assert len(jobs1) == 1
    job = jobs1[0]
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


def test_spider_jobs_run(spider):
    job0 = spider.jobs.run()
    assert isinstance(job0, Job)
    validate_default_meta(job0.metadata, state='pending')
    assert isinstance(job0.metadata.get('pending_time'), int)
    assert job0.metadata.get('pending_time') > 0
    assert job0.metadata.get('scheduled_by')

    with pytest.raises(DuplicateJobError):
        spider.jobs.run()

    job1 = spider.jobs.run(job_args={'arg1': 'val1', 'arg2': 'val2'},
                           priority=3, units=3,
                           meta={'state': 'running', 'meta1': 'val1'},
                           add_tag=['tagA', 'tagB'])
    assert isinstance(job1, Job)
    validate_default_meta(job1.metadata, state='running',
                          units=3, priority=3,
                          tags=['tagA', 'tagB'])
    assert job1.metadata.get('meta1') == 'val1'
    assert job1.metadata.get('spider_args') == {'arg1': 'val1', 'arg2': 'val2'}
    assert isinstance(job1.metadata.get('running_time'), int)
    assert job1.metadata.get('running_time') > 0
    assert job1.metadata.get('started_by')


def test_spider_jobs_get(spider):
    # error on wrong jobkey format
    with pytest.raises(ValueError):
        spider.jobs.get('wrongg')

    # error when using different project id in jobkey
    with pytest.raises(ValueError):
        spider.jobs.get('1/2/3')

    # error when using different spider id in jobkey
    with pytest.raises(ValueError):
        spider.jobs.get(TEST_PROJECT_ID + '/2/3')

    fake_job_id = str(JobKey(spider.project_id, spider._id, 3))
    fake_job = spider.jobs.get(fake_job_id)
    assert isinstance(fake_job, Job)


def test_spider_jobs_summary(spider):
    summary = spider.jobs.summary()
    expected_summary = [{'count': 0, 'name': state, 'summary': []}
                        for state in ['pending', 'running', 'finished']]
    assert summary == expected_summary

    counts = {'pending': 1, 'running': 2, 'finished': 3}
    jobs = defaultdict(list)
    for state in sorted(counts):
        for i in range(counts[state]):
            job = spider.jobs.run(job_args={'subid': state + str(i)},
                                  meta={'state': state})
            jobs[state].append(job.key)
    summary1 = spider.jobs.summary()
    for summ in summary1:
        summ_name, summ_data = summ['name'], summ['summary']
        assert summ['count'] == counts[summ_name]
        assert isinstance(summ_data, list)
        assert len(summ_data) == counts[summ_name]
        summ_jobkeys = sorted([d['key'] for d in summ_data])
        assert summ_jobkeys == sorted(jobs[summ_name])

    # filter by queuename
    summary2 = spider.jobs.summary('running')
    assert summary2['count'] == counts['running']

    # limit by count
    summary3 = spider.jobs.summary('finished', count=1)
    assert summary3['count'] == counts['finished']
    assert len(summary3['summary']) == 1

    # additional jobmeta in response
    summary4 = spider.jobs.summary('finished', jobmeta=['units'])
    assert summary4['count'] == counts['finished']
    assert len(summary4['summary']) == 3
    assert summary4['summary'][0].get('units') == 1


def test_spider_jobs_iter_last(spider):
    lastsumm0 = spider.jobs.iter_last()
    assert isinstance(lastsumm0, types.GeneratorType)
    assert list(lastsumm0) == []

    job1 = spider.jobs.run(meta={'state': 'finished'})
    lastsumm1 = list(spider.jobs.iter_last())
    assert len(lastsumm1) == 1
    assert lastsumm1[0].get('key') == job1.key
    assert lastsumm1[0].get('spider') == TEST_SPIDER_NAME
    assert lastsumm1[0].get('state') == 'finished'
    assert lastsumm1[0].get('elapsed') > 0
    assert lastsumm1[0].get('finished_time') > 0
    assert lastsumm1[0].get('ts') > 0

    # next iter_last should return last spider's job again
    job2 = spider.jobs.run(job_args={'subid': 1},
                           meta={'state': 'finished'})
    lastsumm2 = list(spider.jobs.iter_last())
    assert len(lastsumm2) == 1
    assert lastsumm2[0].get('key') == job2.key
