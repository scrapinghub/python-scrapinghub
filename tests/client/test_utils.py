import os
import pytest
from codecs import encode

import mock

from scrapinghub.client.utils import parse_auth
from scrapinghub.client.proxy import _format_iter_filters


def test_format_iter_filters():
    # work with empty params
    assert _format_iter_filters({}) == {}

    # doesn't affect other params
    params = {'a': 123, 'b': 456}
    assert _format_iter_filters(params) == params

    # pass filter as-is if not list
    params = {'filter': 'some-string'}
    assert _format_iter_filters(params) == params

    # work fine with empty filter
    params = {'filter': []}
    assert _format_iter_filters(params) == params

    # pass string filters as-is
    params = {'filter': ['str1', 'str2']}
    assert _format_iter_filters(params) == params

    # converts list-formatted filters
    params = {'filter': [['field', '>=', ['val']], 'filter2']}
    assert (_format_iter_filters(params) ==
            {'filter': ['["field", ">=", ["val"]]', 'filter2']})

    # works the same with tuple entries
    params = {'filter': [('field', '==', ['val'])]}
    assert (_format_iter_filters(params) ==
            {'filter': ['["field", "==", ["val"]]']})

    # exception if entry is not list/tuple or string
    with pytest.raises(ValueError):
        _format_iter_filters({'filter': ['test', 123]})


def test_parse_auth_none():
    with pytest.raises(RuntimeError):
        parse_auth(None)


@mock.patch.dict(os.environ, {'SH_APIKEY': 'testkey'})
def test_parse_auth_none_with_env():
    assert parse_auth(None) == ('testkey', '')


def test_parse_auth_tuple():
    assert parse_auth(('test', 'test')) == ('test', 'test')
    assert parse_auth(('apikey', '')) == ('apikey', '')

    with pytest.raises(ValueError):
        parse_auth(('user', 'pass', 'bad-param'))

    with pytest.raises(ValueError):
        parse_auth((None, None))

    with pytest.raises(ValueError):
        parse_auth((1234, ''))


def test_parse_auth_not_string():
    with pytest.raises(ValueError):
        parse_auth(12345)


def test_parse_auth_simple():
    assert parse_auth('user:pass') == ('user', 'pass')


def test_parse_auth_apikey():
    apikey = 'c3a3c298c2b8c3a6c291c284c3a9'
    assert parse_auth(apikey) == (apikey, '')


def test_parse_auth_jwt_token():
    test_job, test_token = '1/2/3', 'some.jwt.token'
    raw_token = (test_job + ':' + test_token).encode('utf8')
    encoded_token = encode(raw_token, 'hex_codec').decode('ascii')
    assert parse_auth(encoded_token) == (test_job, test_token)
