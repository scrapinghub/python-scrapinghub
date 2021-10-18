import types
from collections import defaultdict
from collections.abc import Iterator

import pytest
import responses
from six.moves import range
from requests.compat import urljoin

from scrapinghub import ScrapinghubClient
from scrapinghub.client.activity import Activity
from scrapinghub.client.collections import Collections
from scrapinghub.client.exceptions import DuplicateJobError, ServerError
from scrapinghub.client.frontiers import Frontiers
from scrapinghub.client.jobs import Jobs, Job
from scrapinghub.client.projects import Project, Settings
from scrapinghub.client.spiders import Spiders

from scrapinghub.hubstorage.utils import apipoll

from ..conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from ..conftest import TEST_USER_AUTH, TEST_DASH_ENDPOINT
from .utils import validate_default_meta


# Projects class tests


def test_projects_get(client):
    projects = client.projects
    # testing with int project id
    p1 = projects.get(int(TEST_PROJECT_ID))
    assert isinstance(p1, Project)
    # testing with string project id
    p2 = projects.get(TEST_PROJECT_ID)
    assert isinstance(p2, Project)
    assert p1.key == p2.key


def test_projects_list(client):
    projects = client.projects.list()
    assert client.projects.list() == []

    # use user apikey to list test projects
    client = ScrapinghubClient(TEST_USER_AUTH, TEST_DASH_ENDPOINT)
    projects = client.projects.list()
    assert isinstance(projects, list)
    assert int(TEST_PROJECT_ID) in projects


@responses.activate
def test_projects_list_server_error(client):
    url = urljoin(TEST_DASH_ENDPOINT, 'scrapyd/listprojects.json')
    responses.add(responses.GET, url, body='some error body', status=500)
    with pytest.raises(ServerError):
        client.projects.list()
    assert len(responses.calls) == 1


def test_projects_summary(client, project):
    # add at least one running or pending job to ensure summary is returned
    project.jobs.run(TEST_SPIDER_NAME, meta={'state': 'running'})

    def _get_summary():
        summaries = {str(js['project']): js
                     for js in client.projects.summary()}
        return summaries.get(TEST_PROJECT_ID)

    summary = apipoll(_get_summary)
    assert summary is not None


#  Project class tests

def test_project_base(project):
    assert project.key == TEST_PROJECT_ID
    assert isinstance(project.collections, Collections)
    assert isinstance(project.jobs, Jobs)
    assert isinstance(project.spiders, Spiders)
    assert isinstance(project.activity, Activity)
    assert isinstance(project.frontiers, Frontiers)
    assert isinstance(project.settings, Settings)


def test_project_jobs(project):
    jobs = project.jobs
    assert jobs.project_id == TEST_PROJECT_ID
    assert jobs.spider is None


def test_project_jobs_count(project):
    assert project.jobs.count() == 0
    assert project.jobs.count(state=['pending', 'running', 'finished']) == 0

    project.jobs.run(TEST_SPIDER_NAME)
    assert project.jobs.count(state='pending') == 1

    for i in range(2):
        project.jobs.run(TEST_SPIDER_NAME,
                         job_args={'subid': 'running-%s' % i},
                         meta={'state': 'running'})
    assert project.jobs.count(state='running') == 2

    for i in range(3):
        project.jobs.run(TEST_SPIDER_NAME,
                         job_args={'subid': 'finished%s' % i},
                         meta={'state': 'finished'})
    assert project.jobs.count(state='finished') == 3

    assert project.jobs.count(state=['pending', 'running', 'finished']) == 6
    assert project.jobs.count(state='pending') == 1
    assert project.jobs.count(state='running') == 2
    assert project.jobs.count(state='finished') == 3
    assert project.jobs.count() == 3


def test_project_jobs_iter(project):
    project.jobs.run(TEST_SPIDER_NAME, meta={'state': 'running'})

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


def test_project_jobs_list(project):
    project.jobs.run(TEST_SPIDER_NAME, meta={'state': 'running'})

    # no finished jobs
    jobs0 = project.jobs.list()
    assert isinstance(jobs0, list)
    assert len(jobs0) == 0

    # filter by state must work like for iter
    jobs1 = project.jobs.list(state='running')
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


def test_project_jobs_run(project):
    # scheduling on project level requires spidername
    with pytest.raises(ValueError):
        project.jobs.run()

    job0 = project.jobs.run(TEST_SPIDER_NAME)
    assert isinstance(job0, Job)
    validate_default_meta(job0.metadata, state='pending')
    assert isinstance(job0.metadata.get('pending_time'), int)
    assert job0.metadata.get('pending_time') > 0
    assert job0.metadata.get('scheduled_by')

    # running the same spider with same args leads to duplicate error
    with pytest.raises(DuplicateJobError):
        project.jobs.run(TEST_SPIDER_NAME)

    job1 = project.jobs.run(TEST_SPIDER_NAME,
                            job_args={'arg1': 'val1', 'arg2': 'val2'},
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
    assert meta.get('running_time') > 0
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
            job = project.jobs.run(TEST_SPIDER_NAME,
                                   job_args={'subid': state + str(i)},
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

    job1 = project.jobs.run(TEST_SPIDER_NAME, meta={'state': 'finished'})
    lastsumm1 = list(project.jobs.iter_last())
    assert len(lastsumm1) == 1
    assert lastsumm1[0].get('key') == job1.key
    assert lastsumm1[0].get('spider') == TEST_SPIDER_NAME
    assert lastsumm1[0].get('state') == 'finished'
    assert lastsumm1[0].get('elapsed') > 0
    assert lastsumm1[0].get('finished_time') > 0
    assert lastsumm1[0].get('ts') > 0

    # next iter_last should return last spider's job
    job2 = project.jobs.run(TEST_SPIDER_NAME,
                            job_args={'subid': 1},
                            meta={'state': 'finished'})
    lastsumm2 = list(project.jobs.iter_last())
    assert len(lastsumm2) == 1
    assert lastsumm2[0].get('key') == job2.key


def test_settings_get_set(project):
    project.settings.set('job_runtime_limit', 20)
    assert project.settings.get('job_runtime_limit') == 20
    project.settings.set('job_runtime_limit', 24)
    assert project.settings.get('job_runtime_limit') == 24


def test_settings_update(project):
    project.settings.set('job_runtime_limit', 20)
    project.settings.update({'job_runtime_limit': 24})
    assert project.settings.get('job_runtime_limit') == 24


def test_settings_delete(project):
    project.settings.delete('job_runtime_limit')
    assert not project.settings.get('job_runtime_limit')


def test_settings_iter_list(project):
    project.settings.set('job_runtime_limit', 24)
    settings_iter = project.settings.iter()
    assert isinstance(settings_iter, Iterator)
    settings_list = project.settings.list()
    assert ('job_runtime_limit', 24) in settings_list
    assert settings_list == list(settings_iter)
