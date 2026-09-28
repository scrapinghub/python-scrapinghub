import os
import json
import mock
import pytest
import requests

from scrapinghub import APIError
from scrapinghub import Connection
from scrapinghub import Project
from scrapinghub import __version__


def test_connection_class_attrs():
    assert Connection.DEFAULT_ENDPOINT == 'https://app.zyte.com/api/'
    assert isinstance(Connection.API_METHODS, dict)


def test_connection_init_fail_wo_apikey(monkeypatch):
    monkeypatch.delenv('SH_APIKEY', raising=False)
    with pytest.raises(RuntimeError):
        Connection()


@mock.patch.dict(os.environ, {'SH_APIKEY': 'testkey'})
def test_connection_init_use_key_from_env():
    conn = Connection()
    assert conn.apikey == 'testkey'


def test_connection_init_assert_apikey_not_url():
    with pytest.raises(AssertionError):
        Connection(password='testpass', apikey='http://some-url')


def test_connection_init_with_default_url():
    conn = Connection(apikey='testkey')
    assert conn.url == Connection.DEFAULT_ENDPOINT


def test_connection_init_with_default_timeout():
    conn = Connection(apikey='testkey')
    assert conn._connection_timeout is None


def test_connection_init_with_custom_timeout():
    conn = Connection(apikey='testkey', connection_timeout=60)
    assert conn._connection_timeout == 60


def test_connection_init_ok(connection):
    assert connection.apikey == 'testkey'
    assert connection.url == 'http://test-url'
    assert connection._session


def test_connection_repr(connection):
    assert repr(connection) == "Connection('testkey')"


def test_connection_auth(connection):
    assert connection.auth == ('testkey', '')


def test_connection_create_session(connection):
    session = connection._session
    assert isinstance(session, requests.Session)
    assert session.auth == ('testkey', '')
    assert (session.headers.get('User-Agent') ==
            'python-scrapinghub/{}'.format(__version__))
    assert session.stream
    assert not session.prefetch


def test_connection_build_url_unknown_method(connection):
    with pytest.raises(APIError):
        connection._build_url('unknown_method', 'json')


def test_connection_build_url_ok(connection):
    assert (connection._build_url('addversion', 'json') ==
            'http://test-url/scrapyd/addversion.json')


def test_connection_request_wrong_format(connection):
    with pytest.raises(APIError):
        connection._request('http://some-url', 'data', {},
                            'wrongformat', None, None)


def test_connection_get_wo_params(connection):
    connection._request = mock.Mock()
    connection._request.return_value = 'expected'
    assert connection._get('addversion', 'json', headers={'Header': 'value'},
                           raw=True) == 'expected'
    assert connection._request.called
    assert connection._request.call_args_list == [
        (('http://test-url/scrapyd/addversion.json', None,
         {'Header': 'value'}, 'json', True), {})]


def test_connection_get_with_params(connection):
    connection._request = mock.Mock()
    connection._request.return_value = 'expected'
    assert connection._get(
        'addversion', 'json', headers={'Header': 'value'},
        raw=True, params=[('pA', 'vA'), ('pB', 'vB')]) == 'expected'
    assert connection._request.called
    assert connection._request.call_args_list == [
        (('http://test-url/scrapyd/addversion.json?pA=vA&pB=vB', None,
         {'Header': 'value'}, 'json', True), {})]


def test_connection_post(connection):
    connection._request = mock.Mock()
    connection._request.return_value = 'expected'
    assert connection._post(
        'addversion', 'json', headers={'Header': 'value'},
        raw=True, params={'pA': 'vA', 'pB': 'vB'},
        files={'fileA': 'content'}) == 'expected'
    assert connection._request.called
    assert connection._request.call_args_list == [
        (('http://test-url/scrapyd/addversion.json', {'pA': 'vA', 'pB': 'vB'},
         {'Header': 'value'}, 'json', True, {'fileA': 'content'}), {})]


@pytest.mark.parametrize("timeout", [None, 0.1])
def test_connection_request_handle_get(connection, timeout):
    if timeout:
        connection._connection_timeout = timeout
    connection._session = mock.Mock()
    connection._session.get.return_value = 'get_response'
    connection._decode_response = mock.Mock()
    connection._decode_response.return_value = 'expected'
    assert connection._request(
        'http://some-url', None, {'HeaderA': 'value'},
        format='json', raw=False, files=None) == 'expected'
    assert connection._session.get.called
    assert connection._session.get.call_args_list == [
        (('http://some-url',),
         {'headers': {'HeaderA': 'value'}, 'timeout': timeout})]
    assert connection._decode_response.called
    assert connection._decode_response.call_args_list == [
        (('get_response', 'json', False),)]


@pytest.mark.parametrize("timeout", [None, 0.1])
def test_connection_request_handle_post(connection, timeout):
    if timeout:
        connection._connection_timeout = timeout
    connection._session = mock.Mock()
    connection._session.post.return_value = 'post_response'
    connection._decode_response = mock.Mock()
    connection._decode_response.return_value = 'expected'
    assert connection._request(
        'http://some-url', 'data', {'HeaderA': 'value'},
        format='json', raw=True, files={'fileA': 'content'}) == 'expected'
    assert connection._session.post.called
    assert connection._session.post.call_args_list == [
        (('http://some-url',),
         {'headers': {'HeaderA': 'value'}, 'data': 'data',
          'files': {'fileA': 'content'},
          'timeout': timeout})]
    assert connection._decode_response.called
    assert connection._decode_response.call_args_list == [
        (('post_response', 'json', True),)]


def test_connection_decode_response_raw(connection):
    response = mock.Mock()
    response.status_code = 200
    response.raw = 'expected'
    assert connection._decode_response(
        response, 'json', raw=True) == 'expected'


def test_connection_decode_response_json_ok(connection):
    response = mock.Mock()
    response.status_code = 200
    response.text = '{"status":"ok","data":"some-data"}'
    assert connection._decode_response(
        response, 'json', raw=False) == {"status": "ok", "data": "some-data"}


def test_connection_decode_response_json_error(connection):
    response = mock.Mock()
    response.status_code = 400
    for status in ['error', 'badrequest']:
        response.text = json.dumps({"status": status, "message": "error"})
        with pytest.raises(APIError) as exc:
            connection._decode_response(response, 'json', raw=False)
        assert exc.value.args == ("error",)


def test_connection_decode_response_json_unknown(connection):
    response = mock.Mock()
    response.status_code = 400
    response.text = json.dumps({"status": "unexpected", "message": "error"})
    with pytest.raises(APIError) as exc:
        connection._decode_response(response, 'json', raw=False)
    assert exc.value.args == ("Unknown response status: unexpected",)


def test_connection_decode_response_jl(connection):
    jl_data = [{'row1': 'data1'}, {'row2': 'data2'}]
    response = mock.Mock()
    response.status_code = 200
    response.iter_lines.return_value = [json.dumps(x) for x in jl_data]
    assert list(connection._decode_response(
        response, 'jl', raw=False)) == jl_data


def test_connection_getitem(connection):
    project = connection['key']
    assert isinstance(project, Project)
    assert project.id == 'key'


def test_connection_project_ids(connection):
    connection._get = mock.Mock()
    connection._get.return_value = {'projects': [1, 2, 3]}
    assert connection.project_ids() == [1, 2, 3]
    assert connection._get.called
    assert connection._get.call_args_list == [(('listprojects', 'json'), {})]


def test_connection_project_names(connection):
    connection._get = mock.Mock()
    connection._get.return_value = {'projects': [1, 2, 3]}
    assert connection.project_names() == [1, 2, 3]
