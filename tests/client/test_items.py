import mock
import pytest
from six.moves import range

from scrapinghub.client.items import Items

from .utils import normalize_job_for_tests


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
    job = normalize_job_for_tests(job)
    _add_test_items(job, size=10)
    job.finish()

    o = job.items.list_iter(chunksize=3, start=3, count=7)
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


def test_items_list_iter_with_start_and_count_2(spider, json_and_msgpack):
    """2nd version from the test above but this case makes sure that the total
    number of items returned would be equal to `count`.
    """

    job = spider.jobs.run(meta={'state': 'running'})
    job = normalize_job_for_tests(job)
    _add_test_items(job, size=10)
    job.finish()

    o = job.items.list_iter(chunksize=2, start=3, count=3)
    assert next(o) == [
        {'id': 3, 'data': 'data3'},
        {'id': 4, 'data': 'data4'},
    ]
    assert next(o) == [
        {'id': 5, 'data': 'data5'},
    ]
    with pytest.raises(StopIteration):
        next(o)


@pytest.mark.parametrize('meta, keep_key', [
    ({}, False),
    ({'meta': '_key'}, True),
])
def test_items_list_iter_with_skipped_items(meta, keep_key):
    """Items skipped by the server, e.g. due to a filter, must not cause
    list_iter to return the same items more than once."""
    matching = [2, 5, 6, 9]

    def iter_(count, start, meta):
        assert '_key' in meta
        offset = int(start.rsplit('/', 1)[1])
        for index in [i for i in matching if i >= offset][:count]:
            yield {'_key': '1/2/3/{}'.format(index), 'id': index}

    def item(index):
        if keep_key:
            return {'_key': '1/2/3/{}'.format(index), 'id': index}
        return {'id': index}

    items = Items.__new__(Items)
    items.key = '1/2/3'
    with mock.patch.object(Items, 'iter', side_effect=iter_):
        assert list(items.list_iter(chunksize=1, **meta)) == [
            [item(i)] for i in matching
        ] + [[]]
