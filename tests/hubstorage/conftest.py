import os
import vcr
import pytest
import requests
from scrapinghub import HubstorageClient
from scrapinghub.hubstorage.utils import urlpathjoin


TEST_PROJECT_ID = "2222222"
TEST_SPIDER_NAME = 'hs-test-spider'
TEST_FRONTIER_NAME = 'test'
TEST_FRONTIER_SLOT = 'site.com'
TEST_BOTGROUPS = ['python-hubstorage-test', 'g1']
TEST_COLLECTION_NAME = "test_collection_123"

VCR_CASSETES_DIR = 'tests/hubstorage/cassetes'

TEST_AUTH = os.getenv('HS_AUTH', 'f' * 32)
TEST_ENDPOINT = os.getenv('HS_ENDPOINT', 'http://storage.vm.scrapinghub.com')


@pytest.fixture
def vcr_instance(scope='session'):
    return vcr.VCR(
        cassette_library_dir=VCR_CASSETES_DIR,
        record_mode='once',
        match_on=['uri', 'method'],
    )

@pytest.fixture(scope='session')
def hsclient():
    return HubstorageClient(auth=TEST_AUTH, endpoint=TEST_ENDPOINT)


@pytest.fixture(scope='session')
def hsproject(hsclient):
    return hsclient.get_project(TEST_PROJECT_ID)


@pytest.fixture
def hsspiderid(hsproject):
    return str(hsproject.ids.spider(TEST_SPIDER_NAME, create=1))


@pytest.fixture
def hscollection(hsproject):
    return hsproject.collections.new_store(TEST_COLLECTION_NAME)


@pytest.fixture(autouse=True, scope='session')
def setup_session(hsclient, hsproject):
    yield
    hsclient.close()


@pytest.fixture(autouse=True)
def setup_test(hsclient, hsproject, hscollection, request, vcr_instance):
    cassette_name = '{}/{}.yaml'.format(
        request.function.__module__.split('.')[-1],
        request.function.__name__
    )
    with vcr_instance.use_cassette(cassette_name):
        _set_testbotgroup(hsproject)
        _remove_all_jobs(hsproject)
        _clean_collection(hscollection)
        yield
        _remove_all_jobs(hsproject)
        _unset_testbotgroup(hsproject)

# ----------------------------------------------------------------------------

def _clean_collection(hscollection):
    for item in hscollection.iter_values():
        hscollection.delete(item['_key'])


def _set_testbotgroup(hsproject):
    hsproject.settings.apipost(jl={'botgroups': [TEST_BOTGROUPS[0]]})
    # Additional step to populate JobQ's botgroups table
    for botgroup in TEST_BOTGROUPS:
        url = urlpathjoin(TEST_ENDPOINT, 'botgroups', botgroup, 'max_running')
        requests.post(url, auth=hsproject.auth, data='null')
    hsproject.settings.expire()


def _unset_testbotgroup(hsproject):
    hsproject.settings.apidelete('botgroups')
    hsproject.settings.expire()
    # Additional step to delete botgroups in JobQ
    for botgroup in TEST_BOTGROUPS:
        url = urlpathjoin(TEST_ENDPOINT, 'botgroups', botgroup)
        requests.delete(url, auth=hsproject.auth)


def _remove_all_jobs(hsproject):
    for k in list(hsproject.settings.keys()):
        if k != 'botgroups':
            del hsproject.settings[k]
    hsproject.settings.save()

    # Cleanup JobQ
    jobq = hsproject.jobq
    for queuename in ('pending', 'running', 'finished'):
        info = {'summary': [None]}  # poor-guy do...while
        while info['summary']:
            info = jobq.summary(queuename)
            for summary in info['summary']:
                _remove_job(hsproject, summary['key'])


def _remove_job(hsproject, jobkey):
    hsproject.jobq.finish(jobkey)
    hsproject.jobq.delete(jobkey)
    _delete_job(hsproject, jobkey)


def _delete_job(hsproject, jobkey):
    assert jobkey.startswith(TEST_PROJECT_ID), jobkey
    hsproject.jobs.apidelete(jobkey.partition('/')[2])


def start_job(hsproject, **startparams):
    jobdata = hsproject.jobq.start(**startparams)
    if jobdata:
        jobkey = jobdata.pop('key')
        jobauth = (jobkey, jobdata['auth'])
        return hsproject.get_job(jobkey, jobauth=jobauth, metadata=jobdata)
