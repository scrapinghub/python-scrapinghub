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
TEST_BOTGROUP = 'python-hubstorage-test'
TEST_COLLECTION_NAME = "test_collection_123"
TEST_AUTH = os.getenv('HS_AUTH', 'f' * 32)
TEST_ENDPOINT = os.getenv('HS_ENDPOINT', 'http://storage.vm.scrapinghub.com')

VCR_CASSETES_DIR = 'tests/hubstorage/cassetes'

my_vcr = vcr.VCR(cassette_library_dir=VCR_CASSETES_DIR, record_mode='once')


@pytest.fixture(scope='session')
def hsclient():
    return HubstorageClient(auth=TEST_AUTH, endpoint=TEST_ENDPOINT)


@pytest.fixture(scope='session')
def hsproject(hsclient):
    return hsclient.get_project(TEST_PROJECT_ID)


#@my_vcr.use_cassette()
@pytest.fixture(scope='session')
def hsspiderid(hsproject):
    return str(hsproject.ids.spider(TEST_SPIDER_NAME, create=1))



#@my_vcr.use_cassette()
@pytest.fixture(autouse=True, scope='session')
def setup_session(hsclient, hsproject, hscollection):
    set_testbotgroup(hsproject)
    remove_all_jobs(hsproject)
    yield
    remove_all_jobs(hsproject)
    unset_testbotgroup(hsproject)
    hsclient.close()


@pytest.fixture(autouse=True)
def setup_vcrpy_per_test(request, hsproject):
    # generates names like "test_module/test_function.yaml"
    # vcrpy creates the cassetes automatically under VCR_CASSETES_DIR
    cassette_name = '{}/{}.yaml'.format(
        request.function.__module__.split('.')[-1],
        request.function.__name__
    )
    #with my_vcr.use_cassette(cassette_name):
    yield


#@my_vcr.use_cassette()
@pytest.fixture(scope='session')
def hscollection(hsproject):
    collection = get_test_collection(hsproject)
    clean_collection(collection)
    yield collection
    clean_collection(collection)


# ----------------------------------------------------------------------------


def start_job(hsproject, **startparams):
    jobdata = hsproject.jobq.start(**startparams)
    if jobdata:
        jobkey = jobdata.pop('key')
        jobauth = (jobkey, jobdata['auth'])
        return hsproject.get_job(jobkey, jobauth=jobauth, metadata=jobdata)


# Clean environment section


def remove_all_jobs(hsproject):
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
    # delete job
    assert jobkey.startswith(TEST_PROJECT_ID), jobkey
    hsproject.jobs.apidelete(jobkey.partition('/')[2])

# Collection helpers section


def get_test_collection(project):
    return project.collections.new_store(TEST_COLLECTION_NAME)


def clean_collection(collection):
    for item in collection.iter_values():
        collection.delete(item['_key'])


# Botgroups helpers section


def set_testbotgroup(hsproject):
    hsproject.settings.apipost(jl={'botgroups': [TEST_BOTGROUP]})
    # Additional step to populate JobQ's botgroups table
    url = urlpathjoin(TEST_ENDPOINT, 'botgroups', TEST_BOTGROUP, 'max_running')
    requests.post(url, auth=hsproject.auth, data='null')
    hsproject.settings.expire()


def unset_testbotgroup(hsproject):
    hsproject.settings.apidelete('botgroups')
    hsproject.settings.expire()
    # Additional step to delete botgroups in JobQ
    url = urlpathjoin(TEST_ENDPOINT, 'botgroups', TEST_BOTGROUP)
    requests.delete(url, auth=hsproject.auth)
