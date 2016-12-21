import types
import pytest
from collections import defaultdict

from scrapinghub import APIError
from scrapinghub.client import Jobs, Job
from scrapinghub.client import Spider

from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .utils import validate_default_meta


def test_spiders_get(project):
    spider = project.spiders.get(TEST_SPIDER_NAME)
    assert isinstance(spider, Spider)
    assert isinstance(spider.id, int)
    assert spider.name == TEST_SPIDER_NAME
    assert spider.projectid == int(TEST_PROJECT_ID)
    assert isinstance(spider.jobs, Jobs)

    new_spider = project.spiders.get('not-existing')
    assert new_spider.projectid == int(TEST_PROJECT_ID)
    assert new_spider.id != spider.id
    assert new_spider.name == 'not-existing'


def test_spiders_list(project):
    expected_spiders = [{'id': 'hs-test-spider', 'tags': [],
                        'type': 'manual', 'version': None}]
    assert project.spiders.list() == expected_spiders


def test_spider_base(project, spider):
    assert spider.id == 1
    assert spider.name == TEST_SPIDER_NAME
    assert spider.projectid == int(TEST_PROJECT_ID)
    assert isinstance(project.jobs, Jobs)


def test_spider_update_tags(project, spider):
    # empty updates
    assert spider.update_tags() is None
    assert spider.update_tags(
        add=['new1', 'new2'], remove=['old1', 'old2']) == 0

    jobs = [project.jobs.schedule(TEST_SPIDER_NAME, subid='tags-' + str(i))
            for i in range(2)]
    # FIXME the endpoint normalises tags so it's impossible to send tags
    # having upper-cased symbols, let's add more tests when it's fixed
    assert spider.update_tags(add=['tag-a', 'tag-b', 'tag-c']) == 2
    for job in jobs:
        assert job.metadata.liveget('tags') == ['tag-a', 'tag-b', 'tag-c']
    assert spider.update_tags(remove=['tag-c', 'tag-d']) == 2
    for job in jobs:
        assert job.metadata.liveget('tags') == ['tag-a', 'tag-b']
    # FIXME adding and removing tags at the same time doesn't work neither:
    # remove-tag field is ignored if there's non-void add-tag field


def test_spider_jobs(spider):
    jobs = spider.jobs
    assert jobs.projectid == int(TEST_PROJECT_ID)
    assert jobs.spider is spider


def test_spider_jobs_count(spider):
    jobs = spider.jobs
    assert jobs.count() == 0
    assert jobs.count(state=['pending', 'running', 'finished']) == 0

    jobs.schedule()
    assert jobs.count(state='pending') == 1

    for i in range(2):
        jobs.schedule(subid='running-%s' % i, meta={'state': 'running'})
    assert jobs.count(state='running') == 2

    for i in range(3):
        jobs.schedule(subid='finished%s' % i, meta={'state': 'finished'})
    assert jobs.count(state='finished') == 3

    assert jobs.count(state=['pending', 'running', 'finished']) == 6
    assert jobs.count(state='pending') == 1
    assert jobs.count(state='running') == 2
    assert jobs.count(state='finished') == 3
    assert jobs.count() == 3


def test_spider_jobs_iter(spider):
    spider.jobs.schedule(meta={'state': 'running'})

    # no finished jobs
    jobs0 = spider.jobs.iter()
    assert isinstance(jobs0, types.GeneratorType)
    with pytest.raises(StopIteration):
        next(jobs0)

    # filter by state must work
    jobs1 = spider.jobs.iter(state='running')
    job = next(jobs1)
    assert isinstance(job, dict)
    # check: ts/running ts
    ts = job.get('ts')
    assert isinstance(ts, int) and ts > 0
    running_time = job.get('running_time')
    assert isinstance(running_time, int) and running_time > 0
    # check: elapsed time
    elapsed = job.get('elapsed')
    assert isinstance(elapsed, int) and elapsed > 0
    jobkey = job.get('key')
    assert jobkey and jobkey.startswith(TEST_PROJECT_ID)
    assert job.get('spider') == TEST_SPIDER_NAME
    assert job.get('state') == 'running'
    with pytest.raises(StopIteration):
        next(jobs1)


def test_spider_jobs_schedule(spider):
    job0 = spider.jobs.schedule()
    assert isinstance(job0, Job)
    validate_default_meta(job0.metadata, state='pending')
    assert isinstance(job0.metadata.get('pending_time'), int)
    assert job0.metadata['pending_time'] > 0
    assert job0.metadata.get('scheduled_by')

    job1 = spider.jobs.schedule(arg1='val1', arg2='val2', priority=3, units=3,
                                meta={'state': 'running', 'meta1': 'val1'},
                                add_tag=['tagA', 'tagB'])
    assert isinstance(job1, Job)
    validate_default_meta(job1.metadata, state='running', units=3, priority=3,
                          tags=['tagA', 'tagB'])
    assert job1.metadata.get('meta1') == 'val1'
    assert job1.metadata.get('spider_args') == {'arg1': 'val1', 'arg2': 'val2'}
    assert isinstance(job1.metadata.get('running_time'), int)
    assert job1.metadata['running_time'] > 0
    assert job1.metadata.get('started_by')


def test_spider_jobs_get(spider):
    # error when using different project id in jobkey
    with pytest.raises(APIError):
        spider.jobs.get('1/2/3')

    # error when using different spider id in jobkey
    with pytest.raises(APIError):
        spider.jobs.get(TEST_PROJECT_ID + '/2/3')

    fake_job_id = '/'.join([TEST_PROJECT_ID, str(spider.id), '3'])
    fake_job = spider.jobs.get(fake_job_id)
    assert isinstance(fake_job, Job)


def test_spider_jobs_summary(spider):
    summary = spider.jobs.summary()
    expected_summary = [{'count': 0, 'name': state, 'summary': []}
                        for state in ['pending', 'running', 'finished']]
    assert summary == expected_summary

    counts = {'pending': 1, 'running': 2, 'finished': 3}
    jobs = defaultdict(list)
    for state, n in counts.items():
        for i in range(n):
            job = spider.jobs.schedule(subid=state + str(i),
                                       meta={'state': state})
            jobs[state].append(job.key)
    summary1 = spider.jobs.summary()
    for summ in summary1:
        assert summ['count'] == counts[summ['name']]
        summ_data = summ['summary']
        assert isinstance(summ_data, list)
        assert len(summ_data) == counts[summ['name']]
        summ_jobkeys = sorted([d['key'] for d in summ_data])
        assert summ_jobkeys == sorted(jobs[summ['name']])

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


def test_spider_jobs_lastjobsummary(spider):
    lastsumm0 = list(spider.jobs.lastjobsummary())
    assert lastsumm0 == []

    job1 = spider.jobs.schedule(meta={'state': 'finished'})
    lastsumm1 = list(spider.jobs.lastjobsummary())
    assert len(lastsumm1) == 1
    assert lastsumm1[0].get('key') == job1.key
    assert lastsumm1[0].get('spider') == TEST_SPIDER_NAME
    assert lastsumm1[0].get('state') == 'finished'
    assert lastsumm1[0].get('elapsed') > 0
    assert lastsumm1[0].get('finished_time') > 0
    assert lastsumm1[0].get('ts') > 0

    # next lastjobsummary should return last spider's job again
    job2 = spider.jobs.schedule(subid=1,
                                meta={'state': 'finished'})
    lastsumm2 = list(spider.jobs.lastjobsummary())
    assert len(lastsumm2) == 1
    assert lastsumm2[0].get('key') == job2.key
