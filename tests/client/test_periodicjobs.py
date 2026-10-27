import json

import pytest
import responses
from requests.compat import urljoin

from scrapinghub.client.exceptions import BadRequest, NotFound
from scrapinghub.client.periodicjobs import PeriodicJobs

from ..conftest import TEST_DASH_ENDPOINT, TEST_PROJECT_ID


URL = urljoin(TEST_DASH_ENDPOINT,
              'projects/{}/periodicjobs'.format(TEST_PROJECT_ID))
PERIODIC_JOB = {
    'id': 1,
    'disabled': False,
    'project': int(TEST_PROJECT_ID),
    'tasks': [{'name': 'spider1', 'priority': 2, 'spider_args': {},
               'script_args': '', 'jobq_id': 1}],
    'addtags': [],
    'type': 'spider',
    'description': '',
    'cron': '0 9 * * 1',
}


def test_project_periodic_jobs(project):
    assert isinstance(project.periodic_jobs, PeriodicJobs)
    assert project.periodic_jobs.project_id == TEST_PROJECT_ID


@responses.activate
def test_periodic_jobs_list(project):
    job2 = dict(PERIODIC_JOB, id=2)
    responses.add(responses.GET, URL, json={
        'count': 2, 'next': URL + '?page=2', 'previous': None,
        'results': [PERIODIC_JOB], 'meta': {'suggested_hour': '0'}})
    responses.add(responses.GET, URL + '?page=2', json={
        'count': 2, 'next': None, 'previous': URL,
        'results': [job2], 'meta': {'suggested_hour': '0'}})
    assert project.periodic_jobs.list() == [PERIODIC_JOB, job2]
    assert len(responses.calls) == 2


@responses.activate
def test_periodic_jobs_get(project):
    responses.add(responses.GET, URL + '/1', json=PERIODIC_JOB)
    assert project.periodic_jobs.get(1) == PERIODIC_JOB
    responses.add(responses.GET, URL + '/2', status=404)
    with pytest.raises(NotFound):
        project.periodic_jobs.get(2)


@responses.activate
def test_periodic_jobs_create(project):
    responses.add(responses.POST, URL, json=PERIODIC_JOB, status=201)
    tasks = [{'name': 'spider1'}]
    result = project.periodic_jobs.create('0 9 * * 1', tasks, disabled=True)
    assert result == PERIODIC_JOB
    assert json.loads(responses.calls[0].request.body) == {
        'cron': '0 9 * * 1', 'tasks': tasks, 'disabled': True}

    responses.add(responses.POST, URL, status=400)
    with pytest.raises(BadRequest):
        project.periodic_jobs.create('*/15 * * * *', tasks)


@responses.activate
def test_periodic_jobs_update(project):
    updated = dict(PERIODIC_JOB, disabled=True)
    responses.add(responses.PATCH, URL + '/1', json=updated)
    assert project.periodic_jobs.update(1, disabled=True) == updated
    assert json.loads(responses.calls[0].request.body) == {'disabled': True}


@responses.activate
def test_periodic_jobs_delete(project):
    responses.add(responses.DELETE, URL + '/1', status=204)
    assert project.periodic_jobs.delete(1) is None
    assert len(responses.calls) == 1
