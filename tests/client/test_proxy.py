import json
import types
import pytest

from scrapinghub.client import LogLevel
from scrapinghub.hubstorage.serialization import mpdecode


# use some fixed timestamp to represent current time
TEST_TS = 1476803148638


def _add_test_logs(job):
    job.logs.log('simple-msg1')
    job.logs.log('simple-msg2', ts=TEST_TS)
    job.logs.log('simple-msg3', level=LogLevel.DEBUG)

    job.logs.debug('debug-msg')
    job.logs.info('info-msg')
    job.logs.warning('warning-msg')
    job.logs.warn('warn-msg')
    job.logs.error('error-msg')

    # test raw write interface
    job.logs.write({'message': 'test',
                    'level': LogLevel.INFO,
                    'time': TEST_TS})
    job.logs.flush()


def test_logs_base(spider):
    job = spider.jobs.schedule()
    assert list(job.logs.iter()) == []
    assert job.logs.batch_write_start() == 0
    _add_test_logs(job)
    log1 = job.logs.get(0)
    assert log1['level'] == 20
    assert log1['message'] == 'simple-msg1'
    assert isinstance(log1['time'], int) and log1['time'] > 0
    assert job.logs.stats() == {
        'counts': {'10': 2, '20': 4, '30': 2, '40': 1},
        'totals': {'input_bytes': 91, 'input_values': 9}
    }
    job.logs.close()


def test_logs_iter(spider):
    job = spider.jobs.schedule()
    _add_test_logs(job)

    logs = job.logs.iter()
    assert isinstance(logs, types.GeneratorType)
    assert next(logs).get('message') == 'simple-msg1'
    assert next(logs).get('message') == 'simple-msg2'
    assert next(logs).get('message') == 'simple-msg3'

    # testing offset
    logs2 = job.logs.iter(offset=3)
    assert next(logs2).get('message') == 'debug-msg'
    assert next(logs2).get('message') == 'info-msg'

    # testing level
    logs3 = job.logs.iter(level='ERROR')
    assert next(logs3).get('message') == 'error-msg'
    with pytest.raises(StopIteration):
        next(logs3).get('message')


def test_logs_iter_raw_json(spider):
    job = spider.jobs.schedule()
    _add_test_logs(job)

    logs = job.logs.iter_raw_json(offset=2)
    raw_log = next(logs)
    log = json.loads(raw_log)
    assert log.get('message') == 'simple-msg3'
    assert log.get('_key')
    assert isinstance(log.get('time'), int)
    assert log.get('level') == 10

    logs = job.logs.iter_raw_json(level='ERROR')
    raw_log = next(logs)
    log = json.loads(raw_log)
    assert log.get('message') == 'error-msg'


def test_logs_iter_raw_msgpack(spider):
    job = spider.jobs.schedule()
    _add_test_logs(job)

    logs = job.logs.iter_raw_msgpack(offset=2)
    assert isinstance(logs, types.GeneratorType)
    unpacked_logs = list(mpdecode(logs))
    assert unpacked_logs[0].get('message') == 'simple-msg3'

    logs = job.logs.iter_raw_msgpack(level='ERROR')
    unpacked_logs = list(mpdecode(logs))
    assert unpacked_logs[0].get('message') == 'error-msg'


def _add_test_requests(job):
    r1 = job.requests.add(
        url='http://test.com/', status=200, method='GET',
        rs=1337, duration=5, parent=None, ts=TEST_TS)
    job.requests.add(
        url='http://test.com/2', status=400, method='POST',
        rs=0, duration=1, parent=r1, ts=TEST_TS + 1)
    job.requests.add(
        url='http://test.com/3', status=400, method='PUT',
        rs=0, duration=1, parent=r1, ts=TEST_TS + 2, fp='1234')
    job.requests.flush()


def test_requests_iter(spider):
    job = spider.jobs.schedule(meta={'state': 'running'})
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


def test_requests_iter_raw_json(spider):
    job = spider.jobs.schedule()
    _add_test_requests(job)
    job.requests.close()

    rr = job.requests.iter_raw_json()
    raw_req = next(rr)
    req = json.loads(raw_req)
    assert req.get('url') == 'http://test.com/'
    assert req.get('status') == 200
    next(rr)
    next(rr)
    with pytest.raises(StopIteration):
        next(rr)


def test_samples_iter(spider):
    job = spider.jobs.schedule(meta={'state': 'running'})
    assert list(job.samples.iter()) == []
    job.samples.write([TEST_TS, 1, 2, 3])
    job.samples.write([TEST_TS + 1, 5, 9, 4])
    job.samples.flush()
    job.samples.close()
    o = job.samples.iter()
    assert next(o) == [TEST_TS, 1, 2, 3]
    assert next(o) == [TEST_TS + 1, 5, 9, 4]
    with pytest.raises(StopIteration):
        next(o)


def test_items_iter(spider):
    job = spider.jobs.schedule(meta={'state': 'running'})
    for i in range(3):
        job.items.write({'id': i, 'data': 'data' + str(i)})
    job.items.flush()
    job.items.close()

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

    o = job.items.iter_raw_json(offset=2)
    item = json.loads(next(o))
    assert item['id'] == 2
    assert item['data'] == 'data2'
    with pytest.raises(StopIteration):
        next(o)

    msgpacked_o = job.items.iter_raw_msgpack(offset=2)
    o = mpdecode(msgpacked_o)
    assert item['id'] == 2
    assert item['data'] == 'data2'
