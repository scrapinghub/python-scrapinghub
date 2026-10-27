import pytest
from requests import HTTPError, Response

from scrapinghub.client.exceptions import (
    BadRequest,
    Forbidden,
    NotFound,
    ScrapinghubAPIError,
    ServerError,
    Unauthorized,
    ValueTooLarge,
    _wrap_http_errors,
)
from scrapinghub.legacy import APIError


@_wrap_http_errors
def raise_error(error):
    raise error


def http_error(status_code, content=b''):
    response = Response()
    response.status_code = status_code
    response.encoding = 'utf-8'
    response._content = content
    return HTTPError(response=response)


@pytest.mark.parametrize('status_code,expected', [
    (400, BadRequest),
    (401, Unauthorized),
    (403, Forbidden),
    (404, NotFound),
    (413, ValueTooLarge),
    (402, ScrapinghubAPIError),
    (500, ServerError),
    (599, ServerError),
])
def test_wrap_http_errors(status_code, expected):
    with pytest.raises(ScrapinghubAPIError) as excinfo:
        raise_error(http_error(status_code))
    assert type(excinfo.value) is expected


def test_wrap_http_errors_unhandled_status():
    error = http_error(301)
    with pytest.raises(HTTPError) as excinfo:
        raise_error(error)
    assert excinfo.value is error


@pytest.mark.parametrize('error_type,expected', [
    (APIError.ERR_NOT_FOUND, NotFound),
    (APIError.ERR_VALUE_ERROR, ValueError),
    (APIError.ERR_BAD_REQUEST, BadRequest),
    (APIError.ERR_AUTH_ERROR, Unauthorized),
    (APIError.ERR_SERVER_ERROR, ServerError),
    (APIError.ERR_DEFAULT, ScrapinghubAPIError),
])
def test_wrap_api_errors(error_type, expected):
    with pytest.raises(expected) as excinfo:
        raise_error(APIError('some message', _type=error_type))
    assert type(excinfo.value) is expected
    assert str(excinfo.value) == 'some message'


def test_error_message_from_json_payload():
    with pytest.raises(BadRequest) as excinfo:
        raise_error(http_error(400, b'{"message": "wrong value"}'))
    assert str(excinfo.value) == 'wrong value'


def test_error_message_from_response_body():
    with pytest.raises(BadRequest) as excinfo:
        raise_error(http_error(400, b'wrong value'))
    assert str(excinfo.value) == 'wrong value'
