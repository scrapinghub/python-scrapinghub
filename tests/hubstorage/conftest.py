import os
from collections.abc import Iterator
from typing import Any

import vcr
import pytest
import shutil
import requests
from requests import HTTPError

from scrapinghub import HubstorageClient
from scrapinghub.hubstorage.utils import urlpathjoin
from scrapinghub.hubstorage.serialization import MSGPACK_AVAILABLE
from scrapinghub.hubstorage.collectionsrt import Collection
from scrapinghub.hubstorage.job import Job
from scrapinghub.hubstorage.project import Project

from ..conftest import request_accept_header_matcher
from ..conftest import VCRGzipSerializer
from ..conftest import (
    TEST_PROJECT_ID,
    TEST_ENDPOINT,
    TEST_AUTH,
    TEST_BOTGROUP,
    TEST_COLLECTION_NAME,
    TEST_SPIDER_NAME,
)

# vcrpy creates the cassetes automatically under VCR_CASSETES_DIR
VCR_CASSETES_DIR = 'tests/hubstorage/cassetes'

my_vcr = vcr.VCR(cassette_library_dir=VCR_CASSETES_DIR, record_mode='once')
my_vcr.register_serializer('gz', VCRGzipSerializer())
my_vcr.register_matcher('accept_header', request_accept_header_matcher)
my_vcr.serializer = 'gz'
my_vcr.match_on = ('method', 'scheme', 'host', 'port',
                   'path', 'query', 'accept_header')


def pytest_configure(config: pytest.Config) -> None:
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


def is_using_real_services(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.option.update_cassettes or
                request.config.option.ignore_cassettes)


@pytest.fixture(scope='session')
def hsclient() -> HubstorageClient:
    return HubstorageClient(auth=TEST_AUTH, endpoint=TEST_ENDPOINT)


@pytest.fixture(scope='session')
def hsproject(hsclient: HubstorageClient) -> Project:
    return hsclient.get_project(TEST_PROJECT_ID)


@pytest.fixture(scope='session')
@my_vcr.use_cassette()  # type: ignore[untyped-decorator]
def hsspiderid(hsproject: Project) -> str:
    return str(hsproject.ids.spider(TEST_SPIDER_NAME, create=1))


@pytest.fixture(scope='session')
def hscollection(
    hsproject: Project, request: pytest.FixtureRequest,
) -> Iterator[Collection]:
    collection = get_test_collection(hsproject)
    if is_using_real_services(request):
        clean_collection(collection)
    yield collection


@pytest.fixture(autouse=True, scope='session')
def setup_session(
    hsclient: HubstorageClient, hsproject: Project, hscollection: Collection,
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    if is_using_real_services(request):
        set_testbotgroup(hsproject)
        remove_all_jobs(hsproject)
    yield
    hsclient.close()


@pytest.fixture(params=['json', 'msgpack'])
def json_and_msgpack(
    hsclient: HubstorageClient, monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> str:
    if request.param == 'json':
        monkeypatch.setattr(hsclient, 'use_msgpack', False)
    elif not MSGPACK_AVAILABLE or request.config.getoption("--disable-msgpack"):
        pytest.skip("messagepack-based tests are disabled")
    param: str = request.param
    return param


@pytest.fixture(autouse=True)
def setup_vcrpy(
    request: pytest.FixtureRequest, hsproject: Project,
) -> Iterator[None]:
    # generates names like "test_module/test_function.yaml"
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
        remove_all_jobs(hsproject)
    with my_vcr.use_cassette(cassette_name):
        yield


# ----------------------------------------------------------------------------


def start_job(hsproject: Project, **startparams: Any) -> Job:
    jobdata = hsproject.jobq.start(**startparams)
    assert jobdata
    jobkey = jobdata.pop('key')
    jobauth = (jobkey, jobdata['auth'])
    return hsproject.get_job(jobkey, jobauth=jobauth, metadata=jobdata)


# Clean environment section


def remove_all_jobs(hsproject: Project) -> None:
    for k in list(hsproject.settings.keys()):
        if k != 'botgroups':
            del hsproject.settings[k]
    hsproject.settings.save()

    # Cleanup JobQ: run 2 times to ensure we covered all jobs
    for queuename in ('pending', 'running', 'finished')*2:
        info = hsproject.jobq.summary(queuename)
        for summary in info['summary']:
            _remove_job(hsproject, summary['key'])


def _remove_job(hsproject: Project, jobkey: str) -> None:
    hsproject.jobq.finish(jobkey)
    hsproject.jobq.delete(jobkey)
    # delete job
    assert jobkey.startswith(TEST_PROJECT_ID), jobkey
    hsproject.jobs.apidelete(jobkey.partition('/')[2])

# Collection helpers section


def get_test_collection(project: Project) -> Collection:
    return project.collections.new_store(TEST_COLLECTION_NAME)


def clean_collection(collection: Collection) -> None:
    try:
        for item in collection.iter_values():
            collection.delete(item['_key'])
    except HTTPError as e:
        # if collection doesn't exist yet service responds 404
        if e.response is None or e.response.status_code != 404:
            raise


# Botgroups helpers section


def set_testbotgroup(hsproject: Project) -> None:
    hsproject.settings.apipost(jl={'botgroups': [TEST_BOTGROUP]})
    # Additional step to populate JobQ's botgroups table
    url = urlpathjoin(TEST_ENDPOINT, 'botgroups', TEST_BOTGROUP, 'max_running')
    requests.post(url, auth=hsproject.auth, data='null')
    hsproject.settings.expire()


def unset_testbotgroup(hsproject: Project) -> None:
    hsproject.settings.apidelete('botgroups')
    hsproject.settings.expire()
    # Additional step to delete botgroups in JobQ
    url = urlpathjoin(TEST_ENDPOINT, 'botgroups', TEST_BOTGROUP)
    requests.delete(url, auth=hsproject.auth)
