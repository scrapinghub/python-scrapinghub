
from scrapinghub.client import Jobs
from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME


def test_spiders_get(project):
    pass


def test_spiders_list(project):
    pass


def test_spider_base(project, spider):
    assert spider.id == 1
    assert spider.name == TEST_SPIDER_NAME
    assert spider.projectid == int(TEST_PROJECT_ID)
    assert isinstance(project.jobs, Jobs)


def test_spider_update_tags(spider):
    pass


def test_spider_jobs(project):
    pass


def test_spider_jobs_count(project):
    pass


def test_spider_jobs_iter(project):
    pass


def test_spider_jobs_schedule(project):
    pass


def test_spider_jobs_get(project):
    pass


def test_spider_jobs_summary(project):
    pass


def test_spider_jobs_lastjobsummary(project):
    pass
