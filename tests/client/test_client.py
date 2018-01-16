from scrapinghub import Connection
from scrapinghub import HubstorageClient
from scrapinghub import ScrapinghubClient

from scrapinghub.client import DEFAULT_CONNECTION_TIMEOUT
from scrapinghub.client.jobs import Job
from scrapinghub.client.projects import Projects, Project

from .conftest import TEST_PROJECT_ID


# ScrapinghubClient class tests


def test_client_base(client):
    """Base tests for client instance"""
    assert isinstance(client, ScrapinghubClient)
    assert client._hsclient
    assert isinstance(client._hsclient, HubstorageClient)
    assert client._connection
    assert isinstance(client._connection, Connection)
    assert client._connection._connection_timeout == DEFAULT_CONNECTION_TIMEOUT
    assert client.projects
    assert isinstance(client.projects, Projects)


def test_client_get_project(client):
    project = client.get_project(TEST_PROJECT_ID)
    assert isinstance(project, Project)


def test_client_get_job(client):
    fake_job = client.get_job('1/2/3')
    assert isinstance(fake_job, Job)
