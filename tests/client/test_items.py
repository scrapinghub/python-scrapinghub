import pytest
from six.moves import range

from .utils import normalize_job_for_tests


def _add_test_items(job):
    for i in range(3):
        job.items.write({'id': i, 'data': 'data' + str(i)})
    job.items.flush()
    job.items.close()


def test_items_iter(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    _add_test_items(job)

    o = job.items.iter()
    assert next(o) == {'id': 0, 'data': 'data0'}
    assert next(o) == {'id': 1, 'data': 'data1'}
    next(o)
    with pytest.raises(StopIteration):
        next(o)

    o = job.items.iter(offset=2)
    assert next(o) == {'id': 2, 'data': 'data2'}
    with pytest.raises(StopIteration):
        next(o)


def test_items_list(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    job = normalize_job_for_tests(job)
    _add_test_items(job)

    o = job.items.list()
    assert isinstance(o, list)
    assert len(o) == 3
    assert o[0] == {'id': 0, 'data': 'data0'}
    assert o[1] == {'id': 1, 'data': 'data1'}
    assert o[2] == {'id': 2, 'data': 'data2'}


def test_items_list_iter(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    job = normalize_job_for_tests(job)
    _add_test_items(job)

    o = job.items.list_iter(2)
    assert next(o) == [
        {'id': 0, 'data': 'data0'},
        {'id': 1, 'data': 'data1'},
    ]
    assert next(o) == [
        {'id': 2, 'data': 'data2'},
    ]
    with pytest.raises(StopIteration):
        next(o)
