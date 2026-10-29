import pytest

from scrapinghub import Connection
from scrapinghub import Job, JobSet
from scrapinghub import RequestProxyMixin
from scrapinghub import Project


@pytest.fixture
def connection() -> Connection:
    return Connection(apikey='testkey', url='http://test-url')


@pytest.fixture
def proxy_mixin() -> RequestProxyMixin:
    return RequestProxyMixin()


@pytest.fixture
def project(connection: Connection) -> Project:
    return Project(connection, 12345)


@pytest.fixture
def jobset(project: Project) -> JobSet:
    return JobSet(project, param='value')


@pytest.fixture
def job(project: Project) -> Job:
    return Job(project, '1/2/3', {'field': 'data'})
