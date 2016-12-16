from scrapinghub.client import Jobs
from scrapinghub.client import Activity, Collections, Spiders
from scrapinghub.client import Frontier, Settings, Reports

from .conftest import TEST_PROJECT_ID


def test_project_subresources(project):
    assert project.id == int(TEST_PROJECT_ID)
    assert isinstance(project.collections, Collections)
    assert isinstance(project.jobs, Jobs)
    assert isinstance(project.spiders, Spiders)
    assert isinstance(project.activity, Activity)
    assert isinstance(project.frontier, Frontier)
    assert isinstance(project.settings, Settings)
    assert isinstance(project.reports, Reports)


def test_project_jobs(project):
    pass


def test_project_jobs_count(project):
    pass


def test_project_jobs_iter(project):
    pass


def test_project_jobs_schedule(project):
    pass


def test_project_jobs_get(project):
    pass


def test_project_jobs_summary(project):
    pass


def test_project_jobs_lastjobsummary(project):
    pass
