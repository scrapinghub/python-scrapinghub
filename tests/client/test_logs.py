import types
from numbers import Integral

import pytest

from scrapinghub.client.utils import LogLevel

from .conftest import TEST_TS


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


def test_logs_base(spider, json_and_msgpack):
    job = spider.jobs.run()
    assert list(job.logs.iter()) == []
    assert job.logs.batch_write_start() == 0
    _add_test_logs(job)
    log1 = job.logs.get(0)
    assert log1['level'] == 20
    assert log1['message'] == 'simple-msg1'
    assert isinstance(log1['time'], Integral) and log1['time'] > 0
    assert job.logs.stats() == {
        'counts': {'10': 2, '20': 4, '30': 2, '40': 1},
        'totals': {'input_bytes': 91, 'input_values': 9}
    }
    job.logs.close()


def test_logs_iter(spider, json_and_msgpack):
    job = spider.jobs.run()
    _add_test_logs(job)

    logs1 = job.logs.iter()
    assert isinstance(logs1, types.GeneratorType)
    assert next(logs1).get('message') == 'simple-msg1'
    assert next(logs1).get('message') == 'simple-msg2'
    assert next(logs1).get('message') == 'simple-msg3'

    # testing offset
    logs2 = job.logs.iter(offset=3)
    assert next(logs2).get('message') == 'debug-msg'
    assert next(logs2).get('message') == 'info-msg'

    # testing level
    logs3 = job.logs.iter(level='ERROR')
    assert next(logs3).get('message') == 'error-msg'
    with pytest.raises(StopIteration):
        next(logs3).get('message')


def test_logs_list(spider, json_and_msgpack):
    job = spider.jobs.run()
    _add_test_logs(job)

    logs1 = job.logs.list()
    assert isinstance(logs1, list)
    assert len(logs1) == 9
    assert logs1[0].get('message') == 'simple-msg1'
    assert logs1[1].get('message') == 'simple-msg2'
    assert logs1[2].get('message') == 'simple-msg3'

    # testing offset
    logs2 = job.logs.list(offset=3)
    assert len(logs2) == 6
    assert logs2[0].get('message') == 'debug-msg'
    assert logs2[1].get('message') == 'info-msg'

    # testing level
    logs3 = job.logs.list(level='ERROR')
    assert len(logs3) == 1
    assert logs3[0].get('message') == 'error-msg'


def test_logs_list_filter(spider, json_and_msgpack):
    job = spider.jobs.run()
    _add_test_logs(job)

    logs1 = job.logs.list(filter='["message", "contains", ["simple"]]')
    assert isinstance(logs1, list)
    assert len(logs1) == 3
    assert logs1[0].get('message') == 'simple-msg1'

    logs2 = job.logs.list(filter=[['message', 'contains', ['simple']]])
    assert len(logs2) == 3

    logs3 = job.logs.list(filter=[('message', 'contains', ['simple'])])
    assert len(logs3) == 3
