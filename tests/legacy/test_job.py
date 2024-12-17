from unittest import mock
import pytest
import requests

from scrapinghub import Job
from scrapinghub import Project


def test_job_attributes():
    assert Job.MAX_RETRIES == 180
    assert Job.RETRY_INTERVAL == 60


def test_job_init(job):
    assert isinstance(job.project, Project)
    assert job._id == '1/2/3'
    assert job.info == {'field': 'data'}


def test_job_id(job):
    assert job.id == job._id == '1/2/3'


def test_job_repr(job):
    assert repr(job) == "Job(Project(Connection('testkey'), 12345), 1/2/3)"


def test_job_items_max_retries(job):
    job._get = mock.Mock()
    job._get.side_effect = ValueError
    job.MAX_RETRIES = 10
    job.RETRY_INTERVAL = 0
    assert list(job.items()) == []
    assert job._get.called
    assert job._get.call_count == 10


def test_job_items_base_void(job):
    job._get = mock.Mock()
    job._get.return_value = iter([])
    assert list(job.items()) == []
    assert job._get.call_args_list == [
        (('items', 'jl'), {'params': {'offset': 0}})]


def test_job_items_base_with_items(job):
    job._get = mock.Mock()
    job._get.return_value = ['itemA', 'itemB']
    assert list(job.items(offset=50, count=10, meta={'meta': 'data'})) == [
        'itemA', 'itemB']
    assert job._get.call_count == 1
    assert job._get.call_args_list == [
        (('items', 'jl'), {'params': {
            'offset': 50, 'count': 10, 'meta': {'meta': 'data'}}})]


def test_job_items_base_with_retry(job):
    job._get = mock.Mock()
    job.RETRY_INTERVAL = 0

    def fake_first_get():
        yield 'itemA1'
        yield 'itemA2'
        raise ValueError()

    def fake_second_get():
        yield 'itemB1'

    job._get.side_effect = [fake_first_get(), fake_second_get()]
    items = job.items(offset=50, count=10, meta={'meta': 'data'})
    assert next(items) == 'itemA1'
    assert job._get.call_count == 1
    assert job._get.mock_calls == [
        (('items', 'jl'), {'params': {'meta': {'meta': 'data'},
                                      'offset': 50, 'count': 10}})]
    job._get.reset_mock()
    assert next(items) == 'itemA2'
    assert job._get.call_count == 0
    assert next(items) == 'itemB1'
    assert job._get.call_count == 1
    assert job._get.mock_calls == [
        (('items', 'jl'), {'params': {'meta': {'meta': 'data'},
                                      'offset': 52, 'count': 8}})]
    with pytest.raises(StopIteration):
        next(items)


def test_job_update(job):
    job._post = mock.Mock()
    job._post.return_value = {'count': 100}
    assert job.update(field='newvalue', paramB='valueB') == 100
    assert job._post.call_args_list == [
        (('jobs_update', 'json',
          {"field": "newvalue", "paramB": "valueB"}), {})]


def test_job_stop_ok(job):
    job._post = mock.Mock()
    job._post.return_value = {'status': 'ok'}
    assert job.stop()
    assert job._post.call_args_list == [(('jobs_stop', 'json'), {})]


def test_job_stop_error(job):
    job._post = mock.Mock()
    job._post.return_value = {'status': 'error'}
    assert not job.stop()
    assert job._post.call_args_list == [(('jobs_stop', 'json'), {})]


def test_job_delete(job):
    job._post = mock.Mock()
    job._post.return_value = {'count': 1}
    assert job.delete() == 1
    assert job._post.call_args_list == [(('jobs_delete', 'json'), {})]


def test_job_add_report(job):
    job._post = mock.Mock()
    job.add_report('testkey', 'testcontent', content_type='custom/type')
    args = job._post.call_args_list[0]
    assert args[0] == ('reports_add', 'json', {
        'project': 12345, 'job': '1/2/3',
        'key': 'testkey', 'content_type': 'custom/type'})
    content = args[1].get('files').get('content')
    assert len(content) == 2
    assert content[0] == 'report'
    assert isinstance(content[1], requests.compat.StringIO)
    assert content[1].read() == 'testcontent'


def test_job_log(job):
    job._get = mock.Mock()
    job._get.return_value = ['jl logs']
    assert job.log(param='value') == ['jl logs']
    assert job._get.call_args_list == [(('log', 'jl', {'param': 'value'}), {})]


def test_job_request_proxy(job):
    assert job._request_proxy == job.project


def test_job_add_params(job):
    assert job._add_params({'param': 'value'}) == {
        'param': 'value', 'job': '1/2/3'}
