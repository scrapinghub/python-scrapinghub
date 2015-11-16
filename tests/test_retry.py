"""
Test Retry Policy
"""
from hstestcase import HSTestCase
from hubstorage import HubstorageClient
import responses
import json
import re


class RetryTest(HSTestCase):
    @responses.activate
    def test_retry_get_job(self):
        # Prepare
        client = HubstorageClient(auth=self.auth, endpoint=self.endpoint, max_retries=3)
        job_metadata = {'project': self.projectid, 'spider': self.spidername, 'state': 'pending'}

        # setup connector that fails on 2 calls
        attempts = [0]  # use a list for nonlocal mutability used in request_callback

        def request_callback(request):
            attempts[0] += 1

            if attempts[0] < 3:
                return (504, {}, "Timeout")
            else:
                resp_body = dict(job_metadata)
                return (200, {}, json.dumps(resp_body))

        responses.add_callback(
            responses.GET, re.compile(self.endpoint + '/.*'),
            callback=request_callback,
            content_type='application/json',
        )

        # Act
        job2 = client.get_job('%s/%s/%s' % (self.projectid, self.spiderid, 42))

        # Assert
        self.assertEqual(dict(job_metadata), dict(job2.metadata))
        self.assertEqual(attempts[0], 3)
