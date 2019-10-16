import pytest
from six.moves import range


def _add_test_items(job, size=3):
    for i in range(size):
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
    _add_test_items(job)

    o = job.items.list()
    assert isinstance(o, list)
    assert len(o) == 3
    assert o[0] == {'id': 0, 'data': 'data0'}
    assert o[1] == {'id': 1, 'data': 'data1'}
    assert o[2] == {'id': 2, 'data': 'data2'}


def test_items_list_iter(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    _add_test_items(job)
    job.finish()

    o = job.items.list_iter(chunksize=2)
    assert next(o) == [
        {'id': 0, 'data': 'data0'},
        {'id': 1, 'data': 'data1'},
    ]
    assert next(o) == [
        {'id': 2, 'data': 'data2'},
    ]
    with pytest.raises(StopIteration):
        next(o)


def test_items_list_iter_with_start_and_count(spider, json_and_msgpack):
    job = spider.jobs.run(meta={'state': 'running'})
    _add_test_items(job, size=10)
    job.finish()

    o = job.items.list_iter(chunksize=3, start=3, size=7)
    assert next(o) == [
        {'id': 3, 'data': 'data3'},
        {'id': 4, 'data': 'data4'},
        {'id': 5, 'data': 'data5'},
    ]
    assert next(o) == [
        {'id': 6, 'data': 'data6'},
        {'id': 7, 'data': 'data7'},
        {'id': 8, 'data': 'data8'},
    ]
    assert next(o) == [
        {'id': 9, 'data': 'data9'},
    ]
    with pytest.raises(StopIteration):
        next(o)
