"""
Test Retry Policy
"""
from requests import HTTPError
from hstestcase import HSTestCase
from hubstorage import HubstorageClient
import responses
import json
import re

GET = responses.GET
POST = responses.POST


class RetryTest(HSTestCase):
    @responses.activate
    def test_metadata_save_does_retry(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}
        callback_get, attempts_count_get = self.make_request_callback(0, job_metadata)
        callback_post, attempts_count_post = self.make_request_callback(2, job_metadata)

        self.mock_api(method=GET, callback=callback_get)
        self.mock_api(method=POST, callback=callback_post)

        # Act
        err = None

        job = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))
        job.metadata['foo'] = 'bar'
        job.metadata.save()

        # Assert
        self.assertIsNone(err, None)
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
        self.assertIsNone(job, None)
        self.assertEqual(err.response.status_code, 504)
        self.assertEqual(attempts_count[0], 1)

    @responses.activate
    def test_get_job_does_retry(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3)
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
        self.assertIsNone(metadata, None)
        self.assertEqual(err.response.status_code, 504)
        self.assertEqual(attempts_count[0], 1)

    @responses.activate
    def test_get_job_does_fails_on_too_many_retries(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=2)
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
        self.assertIsNone(metadata, None)
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

    def make_request_callback(self, timeout_count, body_on_success):
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
                return (504, {}, "Timeout")
            else:
                resp_body = dict(body_on_success)
                return (200, {}, json.dumps(resp_body))

        return (request_callback, attempts)
