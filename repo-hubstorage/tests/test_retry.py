"""
Test Retry Policy
"""
from six.moves.http_client import BadStatusLine
from requests import HTTPError, ConnectionError
from .hstestcase import HSTestCase
from hubstorage import HubstorageClient
import responses
import json
import re

GET = responses.GET
POST = responses.POST
DELETE = responses.DELETE


class RetryTest(HSTestCase):
    def test_delete_on_hubstorage_api_does_not_404(self):
        # NOTE: The current Hubstorage API does not raise 404 errors on deleting resources that do not exist,
        #       Thus the retry policy does not catch 404 errors when retrying deletes (simplify implementation A LOT).
        #       This test checks that this assumption holds.

        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=0)
        project = client.get_project(projectid=self.projectid)

        # Check frontier delete
        project.frontier.delete_slot('frontier_non_existing', 'slot_non_existing')

        # Check metadata delete
        job = client.push_job(self.projectid, self.spidername)
        job.metadata['foo'] = 'bar'  # Add then delete key, this will trigger an api delete for item foo
        del job.metadata['foo']
        job.metadata.save()

        # Check collections delete
        store = project.collections.new_store('foo')
        store.set({'_key': 'foo'})
        store.delete('bar')

        self.assertTrue(True, "No error have been triggered by calling a delete on resources that do not exist")

    @responses.activate
    def test_retrier_does_not_catch_unwanted_exception(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=2, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback, attempts_count = self.make_request_callback(3, job_metadata, http_error_status=403)

        self.mock_api(callback=callback)

        # Act
        job, metadata, err = None, None, None
        try:
            job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))
            metadata = dict(job.metadata)
        except HTTPError as e:
            err = e

        # Assert
        self.assertIsNone(metadata)
        self.assertIsNotNone(err)
        self.assertEqual(err.response.status_code, 403)
        self.assertEqual(attempts_count[0], 1)

    @responses.activate
    def test_retrier_catches_badstatusline_and_429(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}

        attempts_count = [0]  # use a list for nonlocal mutability used in request_callback

        def request_callback(request):
            attempts_count[0] += 1

            if attempts_count[0] <= 2:
                raise ConnectionError("Connection aborted.", BadStatusLine("''"))
            if attempts_count[0] == 3:
                return (429, {}, u'')
            else:
                resp_body = dict(job_metadata)
                return (200, {}, json.dumps(resp_body))

        self.mock_api(callback=request_callback)

        # Act
        job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))

        # Assert
        self.assertEqual(dict(job_metadata), dict(job.metadata))
        self.assertEqual(attempts_count[0], 4)

    @responses.activate
    def test_api_delete_can_be_set_to_non_idempotent(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback_delete, attempts_count_delete = self.make_request_callback(2, job_metadata)

        self.mock_api(method=DELETE, callback=callback_delete)

        # Act
        job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))

        err = None
        try:
            job.metadata.apidelete('/my/non/idempotent/delete/', is_idempotent=False)
        except HTTPError as e:
            err = e

        # Assert
        self.assertEqual(attempts_count_delete[0], 1)
        self.assertIsNotNone(err)

    @responses.activate
    def test_collection_store_and_delete_are_retried(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3, max_retry_time=1)

        callback_post, attempts_count_post = self.make_request_callback(2, [])
        callback_delete, attempts_count_delete = self.make_request_callback(2, [])

        self.mock_api(method=POST, callback=callback_delete, url_match='/.*/deleted')
        self.mock_api(method=POST, callback=callback_post)  # /!\ default regexp matches all paths, has to be added last

        # Act
        project = client.get_project(self.projectid)
        store = project.collections.new_store('foo')
        store.set({'_key': 'bar', 'content': 'value'})
        store.delete('baz')

        # Assert
        self.assertEqual(attempts_count_post[0], 3)
        self.assertEqual(attempts_count_delete[0], 3)

    @responses.activate
    def test_delete_requests_are_retried(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback_getpost, attempts_count_getpost = self.make_request_callback(0, job_metadata)
        callback_delete, attempts_count_delete = self.make_request_callback(2, job_metadata)

        self.mock_api(method=GET, callback=callback_getpost)
        self.mock_api(method=POST, callback=callback_getpost)
        self.mock_api(method=DELETE, callback=callback_delete)

        # Act
        job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))
        job.metadata['foo'] = 'bar'
        del job.metadata['foo']
        job.metadata.save()

        # Assert
        self.assertEqual(attempts_count_delete[0], 3)

    @responses.activate
    def test_metadata_save_does_retry(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback_get, attempts_count_get = self.make_request_callback(0, job_metadata)
        callback_post, attempts_count_post = self.make_request_callback(2, job_metadata)

        self.mock_api(method=GET, callback=callback_get)
        self.mock_api(method=POST, callback=callback_post)

        # Act
        job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))
        job.metadata['foo'] = 'bar'
        job.metadata.save()

        # Assert
        self.assertEqual(attempts_count_post[0], 3)

    @responses.activate
    def test_push_job_does_not_retry(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3)
        callback, attempts_count = self.make_request_callback(2, {'key': '1/2/3'})

        self.mock_api(POST, callback=callback)

        # Act
        job, err = None, None
        try:
            job = client.push_job(self.projectid, self.spidername)
        except HTTPError as e:
            err = e

        # Assert
        self.assertIsNone(job)
        self.assertIsNotNone(err)
        self.assertEqual(err.response.status_code, 504)
        self.assertEqual(attempts_count[0], 1)

    @responses.activate
    def test_get_job_does_retry(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback, attempts_count = self.make_request_callback(2, job_metadata)

        self.mock_api(callback=callback)

        # Act
        job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))

        # Assert
        self.assertEqual(dict(job_metadata), dict(job.metadata))
        self.assertEqual(attempts_count[0], 3)

    @responses.activate
    def test_get_job_does_fails_if_no_retries(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=0)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback, attempts_count = self.make_request_callback(2, job_metadata)

        self.mock_api(callback=callback)

        # Act
        job, metadata, err = None, None, None
        try:
            job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))
            metadata = dict(job.metadata)
        except HTTPError as e:
            err = e

        # Assert
        self.assertIsNone(metadata)
        self.assertIsNotNone(err)
        self.assertEqual(err.response.status_code, 504)
        self.assertEqual(attempts_count[0], 1)

    @responses.activate
    def test_get_job_does_fails_on_too_many_retries(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=2, max_retry_time=1)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback, attempts_count = self.make_request_callback(3, job_metadata)

        self.mock_api(callback=callback)

        # Act
        job, metadata, err = None, None, None
        try:
            job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))
            metadata = dict(job.metadata)
        except HTTPError as e:
            err = e

        # Assert
        self.assertIsNone(metadata)
        self.assertIsNotNone(err)
        self.assertEqual(err.response.status_code, 504)
        self.assertEqual(attempts_count[0], 3)

    def mock_api(self, method=GET, callback=None, url_match='/.*'):
        """
        Mock an API URL using the responses library.

        Args:
            method (Optional[str]): The HTTP method to mock. Defaults to responses.GET
            callback (function(request) -> response):
            url_match (Optional[str]): The API URL regexp. Defaults to '/.*'.
        """
        responses.add_callback(
            method, re.compile(self.endpoint + url_match),
            callback=callback,
            content_type='application/json',
        )

    def make_request_callback(self, timeout_count, body_on_success, http_error_status=504):
        """
        Make a request callback that timeout a couple of time before returning body_on_success

        Returns:
            A tuple (request_callback, attempts), attempts is an array of size one that contains the number of time
            request_callback has been called.
        """
        attempts = [0]  # use a list for nonlocal mutability used in request_callback

        def request_callback(request):
            attempts[0] += 1

            if attempts[0] <= timeout_count:
                return (http_error_status, {}, "Timeout")
            else:
                resp_body = dict(body_on_success)
                return (200, {}, json.dumps(resp_body))

        return (request_callback, attempts)
