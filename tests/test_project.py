import mock

from scrapinghub import Connection
from scrapinghub import Job, JobSet


def test_project_init(project):
    assert isinstance(project.connection, Connection)
    assert project.id == 12345


def test_project_repr(project):
    assert repr(project) == "Project(Connection('testkey'), 12345)"


def test_project_name(project):
    assert project.name == project.id


def test_project_schedule(project):
    project._post = mock.Mock()
    project._post.return_value = {'jobid': '1/2/3'}
    assert project.schedule('testspider', param='value') == '1/2/3'
    assert project._post.call_args_list == [
        (('schedule', 'json', {'spider': 'testspider', 'param': 'value'}), {})]


def test_project_jobs(project):
    jobset = project.jobs(job=123, count=1)
    assert isinstance(jobset, JobSet)


def test_project_job(project):
    test_job = Job(project, '1/2/3', {})
    project.jobs = mock.Mock()
    project.jobs.return_value = iter([test_job])
    assert project.job('1/2/3') == test_job
    assert project.jobs.call_args_list == [
        ((), {'job': '1/2/3', 'count': 1})]


def test_project_spiders(project):
    project._get = mock.Mock()
    project._get.return_value = {'spiders': ['spiderA']}
    assert project.spiders(param='value') == ['spiderA']
    assert project._get.call_args_list == [
        (('spiders', 'json', {'param': 'value'}), {})]


def test_project_request_proxy(project):
    assert project._request_proxy == project.connection


def test_project_add_params(project):
    assert project._add_params({}) == {'project': project.id}


def test_project_as_project_slybot_wo_output_copy(project):
    project._get = mock.Mock()
    project._get.return_value = 'project-data'
    assert project.autoscraping_project_slybot(
        ('testspider',)) == 'project-data'
    assert project._get.call_args_list == [
        (('as_project_slybot', 'zip', {'spider': ('testspider',)}),
         {'raw': True})]


def test_project_as_project_spider_props_with_start_urls(project):
    project._post = mock.Mock()
    project._post.return_value = {'property': 'value'}
    assert project.autoscraping_spider_properties(
        'testspider', start_urls=['start-url']) == {'property': 'value'}
    assert project._post.call_args_list == [
        (('as_spider_properties', 'json',
          {'spider': 'testspider', 'start_url': ['start-url']}), {})]


def test_project_as_project_spider_props_wo_start_urls(project):
    project._get = mock.Mock()
    project._get.return_value = {'property': 'value'}
    assert project.autoscraping_spider_properties(
        'testspider') == {'property': 'value'}
    assert project._get.call_args_list == [
        (('as_spider_properties', 'json', {'spider': 'testspider'}), {})]
