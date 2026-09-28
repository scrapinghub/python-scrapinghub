import os
import pytest
from codecs import encode

import mock

from scrapinghub.client.utils import (
    parse_auth, parse_job_key, _read_dotenv_auth,
)


@pytest.fixture(autouse=True)
def isolated_auth_env(tmp_path, monkeypatch):
    """Keep auth resolution hermetic: drop any ambient auth env vars and run
    from an empty directory so ``find_dotenv()`` can't pick up a stray ``.env``
    from the developer's working tree."""
    for var in ('SH_APIKEY', 'SHUB_APIKEY', 'SHUB_JOBAUTH'):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)


def test_parse_auth_none():
    with pytest.raises(RuntimeError):
        parse_auth(None)


@mock.patch.dict(os.environ, {'SH_APIKEY': 'testkey'})
def test_parse_auth_none_with_env():
    assert parse_auth(None) == ('testkey', '')


@mock.patch.dict(os.environ, {'SH_APIKEY': 'testkey', 'SHUB_JOBAUTH': 'jwt'})
def test_parse_auth_none_with_multiple_env():
    assert parse_auth(None) == ('testkey', '')


@mock.patch.dict(os.environ, {'SHUB_APIKEY': 'aliaskey'})
def test_parse_auth_none_with_shub_apikey_alias():
    assert parse_auth(None) == ('aliaskey', '')


@mock.patch.dict(os.environ, {'SH_APIKEY': 'primary', 'SHUB_APIKEY': 'alias'})
def test_parse_auth_sh_apikey_takes_precedence_over_alias():
    assert parse_auth(None) == ('primary', '')


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


def test_parse_auth_jwt_token_with_jwt_token_env():
    dummy_test_job, dummy_test_token = '1/2/3', 'some.dummy.jwt.token'
    raw_token = (dummy_test_job + ':' + dummy_test_token).encode('utf8')
    dummy_encoded_token = encode(raw_token, 'hex_codec').decode('ascii')

    test_job, test_token = '1/2/3', 'some.jwt.token'
    raw_token = (test_job + ':' + test_token).encode('utf8')
    encoded_token = encode(raw_token, 'hex_codec').decode('ascii')

    with mock.patch.dict(os.environ, {'SHUB_JOBAUTH': dummy_encoded_token}):
        assert parse_auth(encoded_token) == (test_job, test_token)


def test_parse_auth_none_with_jwt_token_env():
    test_job, test_token = '1/2/3', 'some.jwt.token'
    raw_token = (test_job + ':' + test_token).encode('utf8')
    encoded_token = encode(raw_token, 'hex_codec').decode('ascii')

    with mock.patch.dict(os.environ, {'SHUB_JOBAUTH': encoded_token}):
        assert parse_auth(None) == (test_job, test_token)


def test_read_dotenv_auth_default_path(tmp_path):
    (tmp_path / '.env').write_text('SH_APIKEY=FROMDOTENV\n')

    assert _read_dotenv_auth() == {'SH_APIKEY': 'FROMDOTENV'}
    assert 'SH_APIKEY' not in os.environ  # reading the file must not touch env


def test_read_dotenv_auth_parent_dir(tmp_path, monkeypatch):
    (tmp_path / '.env').write_text('SHUB_APIKEY=FROMPARENT\n')
    subdir = tmp_path / 'project' / 'subdir'
    subdir.mkdir(parents=True)
    monkeypatch.chdir(subdir)

    assert _read_dotenv_auth() == {'SHUB_APIKEY': 'FROMPARENT'}


def test_read_dotenv_auth_custom_path(tmp_path):
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SH_APIKEY=CUSTOMKEY\nSHUB_JOBAUTH=CUSTOMJWT\n')

    assert _read_dotenv_auth(str(env_file)) == {
        'SH_APIKEY': 'CUSTOMKEY', 'SHUB_JOBAUTH': 'CUSTOMJWT',
    }


def test_read_dotenv_auth_only_reads_auth_vars(tmp_path):
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SH_APIKEY=ONLYTHIS\nOTHER_VAR=ignored\n')

    assert _read_dotenv_auth(str(env_file)) == {'SH_APIKEY': 'ONLYTHIS'}
    assert 'OTHER_VAR' not in os.environ


def test_read_dotenv_auth_missing_file(tmp_path):
    assert _read_dotenv_auth(str(tmp_path / 'does-not-exist.env')) == {}


def test_parse_auth_none_reads_dotenv(tmp_path):
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SH_APIKEY=DOTENVKEY\n')

    assert parse_auth(None, dotenv_path=str(env_file)) == ('DOTENVKEY', '')


def test_parse_auth_none_reads_shub_apikey_alias_from_dotenv(tmp_path):
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SHUB_APIKEY=ALIASFROMFILE\n')

    assert parse_auth(None, dotenv_path=str(env_file)) == ('ALIASFROMFILE', '')


def test_parse_auth_none_reads_jobauth_from_dotenv(tmp_path):
    test_job, test_token = '1/2/3', 'some.jwt.token'
    raw_token = (test_job + ':' + test_token).encode('utf8')
    encoded_token = encode(raw_token, 'hex_codec').decode('ascii')
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SHUB_JOBAUTH={}\n'.format(encoded_token))

    with pytest.warns(UserWarning):
        assert parse_auth(None, dotenv_path=str(env_file)) == (test_job, test_token)


def test_parse_auth_env_takes_precedence_over_dotenv(tmp_path, monkeypatch):
    monkeypatch.setenv('SH_APIKEY', 'FROMENV')
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SH_APIKEY=FROMDOTENV\n')

    assert parse_auth(None, dotenv_path=str(env_file)) == ('FROMENV', '')


def test_parse_auth_none_does_not_mutate_environ(tmp_path):
    env_file = tmp_path / 'custom.env'
    env_file.write_text('SH_APIKEY=DOTENVKEY\nSHUB_JOBAUTH=JWT\n')

    parse_auth(None, dotenv_path=str(env_file))

    assert 'SH_APIKEY' not in os.environ
    assert 'SHUB_JOBAUTH' not in os.environ


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
