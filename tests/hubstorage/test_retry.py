"""
Test Retry Policy
"""
import json
import re

import pytest
import responses
from mock import patch
from requests import HTTPError, ConnectionError
from scrapinghub import HubstorageClient
from six.moves.http_client import BadStatusLine

from .conftest import TEST_AUTH, TEST_ENDPOINT
from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME


GET = responses.GET
POST = responses.POST
DELETE = responses.DELETE


@patch.multiple('scrapinghub.HubstorageClient',
                RETRY_DEFAULT_JITTER_MS=1,
                RETRY_DEFAULT_EXPONENTIAL_BACKOFF_MS=1)
def hsclient_with_retries(max_retries=3, max_retry_time=1):
    return HubstorageClient(
        auth=TEST_AUTH, endpoint=TEST_ENDPOINT,
        max_retries=max_retries, max_retry_time=max_retry_time,
    )


@patch('scrapinghub.hubstorage.client.Retrying')
def test_hsclient_with_retries_no_wait(retrying_class):
    hsclient_with_retries()
    assert retrying_class.call_count == 1
    call = retrying_class.call_args_list[0]
    assert call[1]['wait_jitter_max'] == 1
    assert int(call[1]['wait_exponential_multiplier']) == 33


def mock_api(method=GET, callback=None, url_match='/.*'):
    """
    Mock an API URL using the responses library.

    Args:
        method (Optional[str]): The HTTP method to mock. Defaults to
        responses.GET callback (function(request) -> response):
        url_match (Optional[str]): The API URL regexp. Defaults to '/.*'.
    """
    responses.add_callback(
        method, re.compile(TEST_ENDPOINT + url_match),
        callback=callback,
        content_type='application/json',
    )


def make_request_callback(timeout_count, body_on_success,
                          http_error_status=504):
    """Make a request callback that timeout a couple of time before returning
    body_on_success.

    Returns:
        A tuple (request_callback, attempts), attempts is an array of size one
        that contains the number of time request_callback has been called.
    """
    # use a list for nonlocal mutability used in request_callback
    attempts = [0]

    def request_callback(request):
        attempts[0] += 1

        if attempts[0] <= timeout_count:
            return (http_error_status, {}, "Timeout")
        else:
            resp_body = dict(body_on_success)
            return (200, {}, json.dumps(resp_body))

    return (request_callback, attempts)


def test_delete_on_hubstorage_api_does_not_404():
    """Delete a non-existing resource without exceptions.

    The current Hubstorage API does not raise 404 errors on deleting resources
    that do not exist, thus the retry policy does not catch 404 errors when
    retrying deletes (simplify implementation A LOT). This test checks that
    this assumption holds.
    """
    client = hsclient_with_retries(max_retries=0, max_retry_time=None)
    project = client.get_project(projectid=TEST_PROJECT_ID)

    # Check frontier delete
    project.frontier.delete_slot('frontier_non_existing', 'slot_non_existing')

    # Check metadata delete
    job = client.push_job(TEST_PROJECT_ID, TEST_SPIDER_NAME)
    # Add then delete key, this will trigger an api delete for item foo
    job.metadata['foo'] = 'bar'
    del job.metadata['foo']
    job.metadata.save()

    # Check collections delete
    store = project.collections.new_store('foo')
    store.set({'_key': 'foo'})
    store.delete('bar')


@responses.activate
def test_retrier_does_not_catch_unwanted_exception(hsspiderid):
    # Prepare
    client = hsclient_with_retries(max_retries=2)
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback, attempts_count = make_request_callback(
        3, job_metadata, http_error_status=403)
    mock_api(callback=callback)

    # Act
    job, metadata, err = None, None, None
    try:
        job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))
        metadata = dict(job.metadata)
    except HTTPError as e:
        err = e

    # Assert
    assert metadata is None
    assert err is not None
    assert err.response.status_code == 403
    assert attempts_count[0] == 1


@pytest.mark.parametrize('err_code', [408, 429, 502, 503, 504])
@responses.activate
def test_retrier_catches_badstatusline_and_selected_http_errors(hsspiderid, err_code):
    # Prepare
    client = hsclient_with_retries()
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }

    # use a list for nonlocal mutability used in request_callback
    attempts_count = [0]

    def request_callback(request):
        attempts_count[0] += 1

        if attempts_count[0] <= 2:
            raise ConnectionError("Connection aborted.", BadStatusLine("''"))
        if attempts_count[0] == 3:
            return (err_code, {}, u'')
        else:
            resp_body = dict(job_metadata)
            return (200, {}, json.dumps(resp_body))

    mock_api(callback=request_callback)

    # Act
    job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))

    # Assert
    assert dict(job_metadata) == dict(job.metadata)
    assert attempts_count[0] == 4


@responses.activate
def test_api_delete_can_be_set_to_non_idempotent(hsspiderid):
    # Prepare
    client = hsclient_with_retries()
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback_delete, attempts_count_delete = make_request_callback(
        2, job_metadata)

    mock_api(method=DELETE, callback=callback_delete)

    # Act
    job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))

    err = None
    try:
        job.metadata.apidelete('/my/non/idempotent/delete/',
                               is_idempotent=False)
    except HTTPError as e:
        err = e

    # Assert
    assert attempts_count_delete[0] == 1
    assert err is not None


@responses.activate
def test_collection_store_and_delete_are_retried():
    # Prepare
    client = hsclient_with_retries()
    callback_post, attempts_count_post = make_request_callback(2, [])
    callback_delete, attempts_count_delete = make_request_callback(2, [])
    # responses>0.6.1 could reorder callbacks, let's be concrete
    mock_api(method=POST, callback=callback_post, url_match='/.*/foo$')
    mock_api(method=POST, callback=callback_delete, url_match='/.*/deleted$')
    # Act
    project = client.get_project(TEST_PROJECT_ID)
    store = project.collections.new_store('foo')
    store.set({'_key': 'bar', 'content': 'value'})
    store.delete('baz')

    # Assert
    assert attempts_count_post[0] == 3
    assert attempts_count_delete[0] == 3


@responses.activate
def test_delete_requests_are_retried(hsspiderid):
    # Prepare
    client = hsclient_with_retries()
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback_getpost, attempts_count_getpost = make_request_callback(
        0, job_metadata)
    callback_delete, attempts_count_delete = make_request_callback(
        2, job_metadata)

    mock_api(method=GET, callback=callback_getpost)
    mock_api(method=POST, callback=callback_getpost)
    mock_api(method=DELETE, callback=callback_delete)

    # Act
    job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))
    job.metadata['foo'] = 'bar'
    del job.metadata['foo']
    job.metadata.save()

    # Assert
    assert attempts_count_delete[0] == 3


@responses.activate
def test_metadata_save_does_retry(hsspiderid):
    # Prepare
    client = hsclient_with_retries()
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback_get, attempts_count_get = make_request_callback(0, job_metadata)
    callback_post, attempts_count_post = make_request_callback(2, job_metadata)

    mock_api(method=GET, callback=callback_get)
    mock_api(method=POST, callback=callback_post)

    # Act
    job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))
    job.metadata['foo'] = 'bar'
    job.metadata.save()

    # Assert
    assert attempts_count_post[0] == 3


@responses.activate
def test_push_job_does_not_retry():
    # Prepare
    client = hsclient_with_retries()
    callback, attempts_count = make_request_callback(2, {'key': '1/2/3'})

    mock_api(POST, callback=callback)

    # Act
    job, err = None, None
    try:
        job = client.push_job(TEST_PROJECT_ID, TEST_SPIDER_NAME)
    except HTTPError as e:
        err = e

    # Assert
    assert job is None
    assert err is not None
    assert err.response.status_code == 504
    assert attempts_count[0] == 1


@responses.activate
def test_get_job_does_retry(hsspiderid):
    # Prepare
    client = hsclient_with_retries()
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback, attempts_count = make_request_callback(2, job_metadata)

    mock_api(callback=callback)

    # Act
    job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))

    # Assert
    assert dict(job_metadata) == dict(job.metadata)
    assert attempts_count[0] == 3


@responses.activate
def test_get_job_does_fails_if_no_retries(hsspiderid):
    # Prepare
    client = hsclient_with_retries(max_retries=0, max_retry_time=None)
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback, attempts_count = make_request_callback(2, job_metadata)

    mock_api(callback=callback)

    # Act
    job, metadata, err = None, None, None
    try:
        job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))
        metadata = dict(job.metadata)
    except HTTPError as e:
        err = e

    # Assert
    assert metadata is None
    assert err is not None
    assert err.response.status_code == 504
    assert attempts_count[0] == 1


@responses.activate
def test_get_job_does_fails_on_too_many_retries(hsspiderid):
    # Prepare
    client = hsclient_with_retries(max_retries=2, max_retry_time=1)
    job_metadata = {
        'project': TEST_PROJECT_ID,
        'spider': TEST_SPIDER_NAME,
        'state': 'pending',
    }
    callback, attempts_count = make_request_callback(3, job_metadata)

    mock_api(callback=callback)

    # Act
    job, metadata, err = None, None, None
    try:
        job = client.get_job('%s/%s/%s' % (TEST_PROJECT_ID, hsspiderid, 42))
        metadata = dict(job.metadata)
    except HTTPError as e:
        err = e

    # Assert
    assert metadata is None
    assert err is not None
    assert err.response.status_code == 504
    assert attempts_count[0] == 3
