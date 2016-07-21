import mock
import pytest

from scrapinghub import APIError
from scrapinghub import Job
from scrapinghub import Project


def test_jobset_init(jobset):
    assert isinstance(jobset.project, Project)
    assert jobset.params == {'param': 'value'}
    assert jobset._jobs is None


def test_jobset_repr(jobset):
    s = repr(jobset)
    assert s == "JobSet(Project(Connection('testkey'), 12345), param=value)"


def test_jobset_iter(jobset):
    jobset._jobs = [{'id': '1/2/3', 'job_data': 'info'}]
    jobs = list(iter(jobset))
    assert jobs
    assert len(jobs) == 1
    assert isinstance(jobs[0], Job)
    assert jobs[0].project == jobset.project
    assert jobs[0]._id == '1/2/3'
    assert jobs[0].info == jobset._jobs[0]


def test_jobset_count(jobset):
    jobset._get = mock.Mock()
    jobset._get.return_value = {'total': 100}
    assert jobset.count() == 100
    assert jobset._get.call_args_list == [(('jobs_count', 'json'), {})]


def test_jobset_update(jobset):
    jobset._post = mock.Mock()
    jobset._post.return_value = {'count': 100}
    assert jobset.update(param='newvalue', paramB='valueB') == 100
    assert jobset._post.call_args_list == [
        (('jobs_update', 'json',
          {"param": "newvalue", "paramB": "valueB"}), {})]


@mock.patch('scrapinghub.Job')
def test_jobset_stop(job_mock, jobset):
    jobset._jobs = [{'id': '1/2/3'}]
    jobset.stop()
    assert job_mock.return_value.stop.called


@mock.patch('scrapinghub.Job')
def test_jobset_delete(job_mock, jobset):
    jobset._jobs = [{'id': '1/2/3'}]
    jobset.delete()
    assert job_mock.return_value.delete.called


def test_jobset_load_jobs_already_loaded(jobset):
    jobset._jobs = [{'id': '1/2/3'}]
    jobset._load_jobs()


def test_jobset_load_jobs_stop_iteration(jobset):
    jobset._get = mock.Mock()
    jobset._get.return_value = iter([])
    with pytest.raises(APIError) as exc:
        jobset._load_jobs()
    assert 'does not contain status' in exc.value.args[0]


def test_jobset_load_jobs_bad_status(jobset):
    job = {'status': 'bad-status'}
    jobset._get = mock.Mock()
    jobset._get.return_value = iter([job])
    with pytest.raises(APIError) as exc:
        jobset._load_jobs()
    assert 'Unknown response status' in exc.value.args[0]


def test_jobset_load_jobs_base(jobset):
    jobs_iter = iter([{'status': 'ok', 'id': '1/2/3'},
                      {'status': 'ok', 'id': '1/2/4'}])
    jobset._get = mock.Mock()
    jobset._get.return_value = jobs_iter
    jobset._load_jobs()
    assert jobset._jobs == jobs_iter


def test_jobset_request_proxy(jobset):
    assert jobset._request_proxy == jobset.project


def test_jobset_add_params_void(jobset):
    assert jobset._add_params({}) == {'param': 'value'}


def test_jobset_add_params_base(jobset):
    params = {'paramX': 'valueX', 'param': 'newvalue'}
    assert jobset._add_params(params) == {
            'param': 'newvalue', 'paramX': 'valueX'}
