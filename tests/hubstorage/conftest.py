import os
import zlib
import base64
import pickle

import vcr
import mock
import pytest
import shutil
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

# vcrpy creates the cassetes automatically under VCR_CASSETES_DIR
VCR_CASSETES_DIR = 'tests/hubstorage/cassetes'


class VCRGzipSerializer(object):
    """Custom ZIP serializer for VCR.py."""

    def serialize(self, cassette_dict):
        # receives a dict, must return a string
        # there can be binary data inside some of the requests,
        # so it's impossible to use json for serialization to string
        compressed = zlib.compress(pickle.dumps(cassette_dict, protocol=2))
        return base64.b64encode(compressed).decode('utf8')

    def deserialize(self, cassette_string):
        # receives a string, must return a dict
        decoded = base64.b64decode(cassette_string.encode('utf8'))
        return pickle.loads(zlib.decompress(decoded))


my_vcr = vcr.VCR(cassette_library_dir=VCR_CASSETES_DIR, record_mode='once')
my_vcr.register_serializer('gz', VCRGzipSerializer())
my_vcr.serializer = 'gz'


def pytest_addoption(parser):
    parser.addoption(
        "--update-cassettes", action="store_true", default=False,
        help="test with real services rewriting existing vcr cassettes")
    parser.addoption(
        "--ignore-cassettes", action="store_true", default=False,
        help="test with real services skipping existing vcr cassettes")


def pytest_configure(config):
    if config.option.update_cassettes:
        # there's vcr `all` mode to update cassettes but it doesn't delete
        # or clear existing records, so its size will always only grow
        if os.path.exists(VCR_CASSETES_DIR):
            shutil.rmtree(VCR_CASSETES_DIR)
    elif config.option.ignore_cassettes:
        # simple hack to just ignore vcr cassettes:
        # - all record_mode means recording new interactions + no replay
        # - before_record returning None means skipping all the requests
        global my_vcr
        my_vcr.record_mode = 'all'
        my_vcr.before_record_request = lambda request: None


def is_using_real_services(request):
    return (request.config.option.update_cassettes or
            request.config.option.ignore_cassettes)


@pytest.fixture(scope='session')
def hsclient():
    return HubstorageClient(auth=TEST_AUTH, endpoint=TEST_ENDPOINT)


@pytest.fixture(scope='session')
def hsproject(hsclient):
    return hsclient.get_project(TEST_PROJECT_ID)


@my_vcr.use_cassette()
@pytest.fixture(scope='session')
def hsspiderid(hsproject):
    return str(hsproject.ids.spider(TEST_SPIDER_NAME, create=1))


@pytest.fixture(scope='session')
def hscollection(hsproject, request):
    collection = get_test_collection(hsproject)
    if is_using_real_services(request):
        clean_collection(collection)
    yield collection


@pytest.fixture(autouse=True, scope='session')
def setup_session(hsclient, hsproject, hscollection, request):
    if is_using_real_services(request):
        set_testbotgroup(hsproject)
        remove_all_jobs(hsproject)
    yield
    hsclient.close()


@pytest.fixture(autouse=True)
def setup_vcrpy(request, hsproject):
    # generates names like "test_module/test_function.yaml"
    # otherwise it uses current function name (setup_vcrpy) for all tests
    # other option is to add vcr decorator to each test separately
    cassette_name = '{}/{}.gz'.format(
        request.function.__module__.split('.')[-1],
        request.function.__name__
    )
    if is_using_real_services(request):
        remove_all_jobs(hsproject)
    with my_vcr.use_cassette(cassette_name):
        yield


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

    # Cleanup JobQ: run 2 times to ensure we covered all jobs
    for queuename in ('pending', 'running', 'finished')*2:
        info = hsproject.jobq.summary(queuename)
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
