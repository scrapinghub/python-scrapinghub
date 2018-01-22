import os
import pytest
from codecs import encode

import mock

from scrapinghub.client.utils import parse_auth, parse_job_key


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


def test_parse_job_key():
    job_key = parse_job_key('123/10/11')
    assert job_key.project_id == '123'
    assert job_key.spider_id == '10'
    assert job_key.job_id == '11'


def test_parse_job_key_non_numeric():
    with pytest.raises(ValueError):
        parse_job_key('123/a/6')


def test_parse_job_key_incorrect_length():
    with pytest.raises(ValueError):
        parse_job_key('123/1')
