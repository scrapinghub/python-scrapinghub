import os
import zlib
import base64
import pickle

import vcr
import pytest
import shutil

from scrapinghub import ScrapinghubClient
from scrapinghub.client.exceptions import NotFound
from scrapinghub.hubstorage.serialization import MSGPACK_AVAILABLE

from ..conftest import request_accept_header_matcher


TEST_PROJECT_ID = "2222222"
TEST_SPIDER_NAME = 'hs-test-spider'
TEST_FRONTIER_SLOT = 'site.com'
TEST_BOTGROUP = 'python-hubstorage-test'
TEST_COLLECTION_NAME = "test_collection_123"
TEST_ADMIN_AUTH = os.getenv('AUTH', 'f' * 32)
TEST_USER_AUTH = os.getenv('USER_AUTH', 'e' * 32)
TEST_DASH_ENDPOINT = os.getenv('DASH_ENDPOINT', 'http://33.33.33.51:8080/api/')
TEST_HS_ENDPOINT = os.getenv('HS_ENDPOINT',
                             'http://storage.vm.scrapinghub.com')

# use some fixed timestamp to represent current time
TEST_TS = 1476803148638

# vcrpy creates the cassetes automatically under VCR_CASSETES_DIR
VCR_CASSETES_DIR = 'tests/client/cassetes'


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
my_vcr.register_matcher('accept_header', request_accept_header_matcher)
my_vcr.serializer = 'gz'
my_vcr.match_on = ('method', 'scheme', 'host', 'port',
                   'path', 'query', 'accept_header')


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
def client():
    return ScrapinghubClient(auth=TEST_ADMIN_AUTH,
                             endpoint=TEST_HS_ENDPOINT,
                             dash_endpoint=TEST_DASH_ENDPOINT)


@pytest.fixture(scope='session')
def project(client):
    return client.get_project(TEST_PROJECT_ID)


@my_vcr.use_cassette()
@pytest.fixture(scope='session')
def spider(project, request):
    # on normal conditions you can't create a new spider this way:
    # it can only be created on project deploy as usual
    spider = project.spiders.get(TEST_SPIDER_NAME, create=True)
    if is_using_real_services(request):
        existing_tags = spider.list_tags()
        if existing_tags:
            spider.update_tags(remove=existing_tags)
    return spider


@pytest.fixture(scope='session')
def collection(project, request):
    collection = get_test_collection(project)
    if is_using_real_services(request):
        clean_collection(collection)
    yield collection


@pytest.fixture(scope='function')
def frontier(project, request, frontier_name):
    frontier = project.frontiers.get(frontier_name)
    if is_using_real_services(request):
        clean_frontier_slot(frontier)
    yield frontier


@pytest.fixture(autouse=True, scope='session')
def setup_session(client, project, collection, request):
    if is_using_real_services(request):
        remove_all_jobs(project)
    yield
    client.close()


@pytest.fixture(params=['json', 'msgpack'])
def json_and_msgpack(client, monkeypatch, request):
    if request.param == 'json':
        monkeypatch.setattr(client._hsclient, 'use_msgpack', False)
    elif not MSGPACK_AVAILABLE or request.config.getoption("--disable-msgpack"):
        pytest.skip("messagepack-based tests are disabled")
    return request.param


@pytest.fixture(autouse=True)
def setup_vcrpy(request, project):
    # generates names like "test_module/test_function{-json}.yaml"
    # otherwise it uses current function name (setup_vcrpy) for all tests
    # other option is to add vcr decorator to each test separately
    serializer_suffix = ''
    if ('json_and_msgpack' in request.fixturenames and
            request.getfixturevalue('json_and_msgpack') == 'json'):
        serializer_suffix = '-json'
    cassette_name = '{}/{}{}.gz'.format(
        request.function.__module__.split('.')[-1],
        request.function.__name__,
        serializer_suffix
    )
    if is_using_real_services(request):
        remove_all_jobs(project)
    with my_vcr.use_cassette(cassette_name):
        yield


# ----------------------------------------------------------------------------


# Clean environment section


def remove_all_jobs(project):
    for k, _ in project.settings.iter():
        if k != 'botgroups':
            project.settings.delete(k)

    # Cleanup JobQ: run 2 times to ensure we covered all jobs
    for queuename in ('pending', 'running', 'finished')*2:
        info = project.jobs.summary(queuename)
        for summary in info['summary']:
            _remove_job(project, summary['key'])


def _remove_job(project, jobkey):
    job = project.jobs.get(jobkey)
    job.finish()
    job.delete()
    # delete job
    assert jobkey.startswith(TEST_PROJECT_ID), jobkey
    hsproject = project._client._hsclient.get_project(TEST_PROJECT_ID)
    hsproject.jobs.apidelete(jobkey.partition('/')[2])

# Collection helpers section


def get_test_collection(project):
    return project.collections.get_store(TEST_COLLECTION_NAME)


def clean_collection(collection):
    try:
        for item in collection.iter():
            collection.delete(item['_key'])
    except NotFound:
        pass


# Frontier helpers section

def clean_frontier_slot(frontier):
    frontier.get(TEST_FRONTIER_SLOT).delete()
