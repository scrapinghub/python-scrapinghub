"""
Test Client
"""
from scrapinghub import HubstorageClient
from scrapinghub.hubstorage.utils import apipoll

from .conftest import TEST_AUTH, TEST_ENDPOINT
from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .conftest import start_job


def test_default_ua(hsclient):
    assert hsclient.user_agent == HubstorageClient.DEFAULT_USER_AGENT


def test_custom_ua():
    client = HubstorageClient(auth=TEST_AUTH,
                              endpoint=TEST_ENDPOINT,
                              user_agent='testUA')
    assert client.user_agent == 'testUA'


def test_push_job(hsclient, hsproject):
    hsclient.push_job(
        TEST_PROJECT_ID, TEST_SPIDER_NAME,
        priority=hsproject.jobq.PRIO_LOW,
        foo='baz',
    )
    job = start_job(hsproject)
    meta = job.metadata
    assert meta.get('state') == u'running', hsclient.auth
    assert meta.get('foo') == u'baz'
    hsproject.jobq.finish(job)
    hsproject.jobq.delete(job)

    # job auth token is valid only while job is running
    meta = hsclient.get_job(job.key).metadata
    assert meta.get('state') == u'deleted'
    assert meta.get('foo') == u'baz'


def test_jobsummaries(hsclient):
    # add at least one running or pending job to ensure summary is returned
    hsclient.push_job(TEST_PROJECT_ID, TEST_SPIDER_NAME, state='running')

    def _get_summary():
        jss = hsclient.projects.jobsummaries()
        mjss = dict((str(js['project']), js) for js in jss)
        return mjss.get(TEST_PROJECT_ID)

    summary = apipoll(_get_summary)
    assert summary is not None


def test_timestamp(hsclient):
    ts1 = hsclient.server_timestamp()
    ts2 = hsclient.server_timestamp()
    assert ts1 > 0
    assert ts1 <= ts2
