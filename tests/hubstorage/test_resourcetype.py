import json

import pytest
from requests.exceptions import ChunkedEncodingError

from scrapinghub import HubstorageClient
from scrapinghub.hubstorage.job import Items
from scrapinghub.hubstorage.serialization import MSGPACK_AVAILABLE, mpdecode

ITEMS = [{'_key': '1/2/3/%d' % i, 'field': 'x' * 20} for i in range(20)]
KEYS = [item['_key'] for item in ITEMS]


def _serve(params):
    if 'startafter' in params:
        start = KEYS.index(params['startafter']) + 1
    elif 'start' in params:
        start = KEYS.index(params['start'])
    else:
        start = 0
    count = params.get('count')
    return ITEMS[start:None if count is None else start + int(count)]


def _flaky_server(serialize, calls=None, full_first=False):
    """Return a replacement for a raw iteration method whose first response
    breaks halfway through, or right after its last chunk if *full_first* is
    true."""
    calls = [] if calls is None else calls

    def iter_raw(_path, params=None, **kwargs):
        chunks = serialize(_serve(params))
        if not calls and not full_first:
            chunks = chunks[:len(chunks) // 2]
        calls.append(dict(params))
        for chunk in chunks:
            yield chunk
        if len(calls) == 1:
            raise ChunkedEncodingError('connection broken')

    return iter_raw


def _msgpack_chunks(items):
    import msgpack

    data = b''.join(msgpack.packb(item) for item in items)
    return [data[i:i + 7] for i in range(0, len(data), 7)]


def _json_lines(items):
    return [json.dumps(item) for item in items]


@pytest.mark.parametrize('use_msgpack', [
    pytest.param(True, marks=pytest.mark.skipif(
        not MSGPACK_AVAILABLE, reason='msgpack not installed')),
    False,
])
@pytest.mark.parametrize('params,expected', [
    ({}, ITEMS),
    ({'start': KEYS[5]}, ITEMS[5:]),
    ({'count': 15}, ITEMS[:15]),
    ({'start': KEYS[5], 'count': 10}, ITEMS[5:15]),
])
def test_iter_values_resumes(use_msgpack, params, expected):
    client = HubstorageClient(auth='apikey', use_msgpack=use_msgpack)
    resource = Items(client, '1/2/3')
    resource.RETRY_INTERVAL = 0
    resource._iter_content = _flaky_server(_msgpack_chunks)
    resource._iter_lines = _flaky_server(_json_lines)
    assert list(resource.iter_values(**params)) == expected


@pytest.mark.skipif(not MSGPACK_AVAILABLE, reason='msgpack not installed')
def test_iter_msgpack_resumes():
    client = HubstorageClient(auth='apikey')
    resource = Items(client, '1/2/3')
    resource.RETRY_INTERVAL = 0
    resource._iter_content = _flaky_server(_msgpack_chunks)
    assert list(mpdecode(resource.iter_msgpack())) == ITEMS


def test_iter_values_stops_after_count():
    client = HubstorageClient(auth='apikey', use_msgpack=False)
    resource = Items(client, '1/2/3')
    resource.RETRY_INTERVAL = 0
    calls = []
    resource._iter_lines = _flaky_server(_json_lines, calls, full_first=True)
    assert list(resource.iter_values(count=5)) == ITEMS[:5]
    assert len(calls) == 1
