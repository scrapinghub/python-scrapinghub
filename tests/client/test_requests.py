import pytest

from .conftest import TEST_TS


def _add_test_requests(job):
    r1 = job.requests.add(
        url='http://test.com/', status=200, method='GET',
        rs=1337, duration=5, ts=TEST_TS)
    job.requests.add(
        url='http://test.com/2', status=400, method='POST',
        rs=0, duration=1, parent=r1, ts=TEST_TS + 1)
    job.requests.add(
        url='http://test.com/3', status=400, method='PUT',
        rs=0, duration=1, parent=r1, ts=TEST_TS + 2, fp='1234')
    job.requests.flush()


def test_requests_iter(spider):
    job = spider.jobs.run(meta={'state': 'running'})
    _add_test_requests(job)
    job.requests.close()
    rr = job.requests.iter()
    assert next(rr) == {
        'url': 'http://test.com/', 'status': 200, 'rs': 1337,
        'time': TEST_TS, 'duration': 5, 'method': 'GET',
    }
    assert next(rr) == {
        'url': 'http://test.com/2', 'status': 400, 'rs': 0,
        'time': TEST_TS + 1, 'duration': 1, 'method': 'POST',
        'parent': 0,
    }
    assert next(rr) == {
        'url': 'http://test.com/3', 'status': 400, 'rs': 0,
        'time': TEST_TS + 2, 'method': 'PUT', 'fp': '1234',
        'parent': 0, 'duration': 1,
    }
    with pytest.raises(StopIteration):
        next(rr)
