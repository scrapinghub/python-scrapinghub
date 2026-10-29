import mock

from scrapinghub import Connection
from scrapinghub import Job, JobSet
from scrapinghub.legacy import Project


def test_project_init(project: Project) -> None:
    assert isinstance(project.connection, Connection)
    assert project.id == 12345


def test_project_repr(project: Project) -> None:
    assert repr(project) == "Project(Connection('testkey'), 12345)"


def test_project_name(project: Project) -> None:
    assert project.name == project.id


def test_project_schedule(project: Project) -> None:
    project._post = mock.Mock()  # type: ignore[method-assign]
    project._post.return_value = {'jobid': '1/2/3'}
    assert project.schedule('testspider', param='value') == '1/2/3'
    assert project._post.call_args_list == [
        (('schedule', 'json', {'spider': 'testspider', 'param': 'value'}), {})]


def test_project_jobs(project: Project) -> None:
    jobset = project.jobs(job=123, count=1)
    assert isinstance(jobset, JobSet)


def test_project_job(project: Project) -> None:
    test_job = Job(project, '1/2/3', {})
    project.jobs = mock.Mock()  # type: ignore[method-assign]
    project.jobs.return_value = iter([test_job])
    assert project.job('1/2/3') == test_job
    assert project.jobs.call_args_list == [
        ((), {'job': '1/2/3', 'count': 1})]


def test_project_job_not_found(project: Project) -> None:
    project.jobs = mock.Mock()  # type: ignore[method-assign]
    project.jobs.return_value = iter([])
    assert project.job('1/2/3') is None


def test_project_spiders(project: Project) -> None:
    project._get = mock.Mock()  # type: ignore[method-assign]
    project._get.return_value = {'spiders': ['spiderA']}
    assert project.spiders(param='value') == ['spiderA']
    assert project._get.call_args_list == [
        (('spiders', 'json', {'param': 'value'}), {})]


def test_project_request_proxy(project: Project) -> None:
    assert project._request_proxy == project.connection


def test_project_add_params(project: Project) -> None:
    assert project._add_params({}) == {'project': project.id}


def test_project_as_project_slybot_wo_output_copy(project: Project) -> None:
    project._get = mock.Mock()  # type: ignore[method-assign]
    project._get.return_value = 'project-data'
    assert project.autoscraping_project_slybot(
        ('testspider',)) == 'project-data'
    assert project._get.call_args_list == [
        (('as_project_slybot', 'zip', {'spider': ('testspider',)}),
         {'raw': True})]


def test_project_as_project_spider_props_with_start_urls(
    project: Project,
) -> None:
    project._post = mock.Mock()  # type: ignore[method-assign]
    project._post.return_value = {'property': 'value'}
    assert project.autoscraping_spider_properties(
        'testspider', start_urls=['start-url']) == {'property': 'value'}
    assert project._post.call_args_list == [
        (('as_spider_properties', 'json',
          {'spider': 'testspider', 'start_url': ['start-url']}), {})]


def test_project_as_project_spider_props_wo_start_urls(
    project: Project,
) -> None:
    project._get = mock.Mock()  # type: ignore[method-assign]
    project._get.return_value = {'property': 'value'}
    assert project.autoscraping_spider_properties(
        'testspider') == {'property': 'value'}
    assert project._get.call_args_list == [
        (('as_spider_properties', 'json', {'spider': 'testspider'}), {})]
