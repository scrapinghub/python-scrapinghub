from scrapinghub import Connection
from scrapinghub import HubstorageClient
from scrapinghub import ScrapinghubClient

from scrapinghub.client import Projects, Project, Job

from scrapinghub.hubstorage.utils import apipoll
from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .conftest import TEST_USER_AUTH, TEST_DASH_ENDPOINT


# ScrapinghubClient class tests


def test_client_base(client):
    """Base tests for client instance"""
    assert isinstance(client, ScrapinghubClient)
    assert client._hsclient
    assert isinstance(client._hsclient, HubstorageClient)
    assert client._connection
    assert isinstance(client._connection, Connection)
    assert client.projects
    assert isinstance(client.projects, Projects)


def test_client_get_project(client):
    project = client.get_project(TEST_PROJECT_ID)
    assert isinstance(project, Project)


def test_client_get_job(client):
    fake_job = client.get_job('1/2/3')
    assert isinstance(fake_job, Job)


# Projects class tests


def test_client_projects_get_project(client):
    projects = client.projects
    # testing with int project id
    p1 = projects.get(int(TEST_PROJECT_ID))
    assert isinstance(p1, Project)
    # testing with string project id
    p2 = projects.get(TEST_PROJECT_ID)
    assert isinstance(p2, Project)
    assert p1.id == p2.id


def test_client_projects_list_projects(client):
    projects = client.projects.list()
    assert client.projects.list() == []

    # use user apikey to list test projects
    client = ScrapinghubClient(TEST_USER_AUTH, TEST_DASH_ENDPOINT)
    projects = client.projects.list()
    assert isinstance(projects, list)
    assert int(TEST_PROJECT_ID) in projects


def test_client_projects_summary(client, project):
    # add at least one running or pending job to ensure summary is returned
    project.jobs.schedule(TEST_SPIDER_NAME, meta={'state': 'running'})

    def _get_summary():
        summaries = {str(js['project']): js
                     for js in client.projects.summary()}
        return summaries.get(TEST_PROJECT_ID)

    summary = apipoll(_get_summary)
    assert summary is not None
