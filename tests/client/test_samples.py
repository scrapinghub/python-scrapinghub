import pytest

from .conftest import TEST_TS


def _add_test_samples(job):
    job.samples.write([TEST_TS, 1, 2, 3])
    job.samples.write([TEST_TS + 1, 5, 9, 4])
    job.samples.flush()
    job.samples.close()


def test_samples_iter(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    assert list(job.samples.iter()) == []
    _add_test_samples(job)

    o = job.samples.iter()
    assert next(o) == [TEST_TS, 1, 2, 3]
    assert next(o) == [TEST_TS + 1, 5, 9, 4]
    with pytest.raises(StopIteration):
        next(o)


def test_samples_list(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    _add_test_samples(job)
    o = job.samples.list()
    assert isinstance(o, list)
    assert len(o) == 2
    assert o[0] == [TEST_TS, 1, 2, 3]
    assert o[1] == [TEST_TS + 1, 5, 9, 4]
