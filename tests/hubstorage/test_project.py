"""
Test Project
"""
import six
import json
import pytest
from requests.exceptions import HTTPError

from scrapinghub import HubstorageClient

from ..conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .conftest import hsspiderid
from .conftest import start_job
from .conftest import set_testbotgroup, unset_testbotgroup
from .testutil import failing_downloader


def test_projectid(hsclient):
    p1 = hsclient.get_project(int(TEST_PROJECT_ID))
    p2 = hsclient.get_project(str(TEST_PROJECT_ID))
    assert p1.projectid == p2.projectid
    assert isinstance(p1.projectid, str)
    assert isinstance(p2.projectid, str)
    with pytest.raises(AssertionError):
        hsclient.get_project('111/3')


def test_get_job_from_key(hsclient, hsproject, hsspiderid):
    job = hsproject.push_job(TEST_SPIDER_NAME)
    parts = tuple(job.key.split('/'))
    assert len(parts) == 3
    assert parts[:2] == (TEST_PROJECT_ID, hsspiderid)
    samejob1 = hsclient.get_job(job.key)
    samejob2 = hsproject.get_job(job.key)
    samejob3 = hsproject.get_job(parts[1:])
    assert samejob1.key == job.key
    assert samejob2.key == job.key
    assert samejob3.key == job.key


def test_get_jobs(hsproject):
    p = hsproject
    j1 = p.push_job(TEST_SPIDER_NAME, testid=0)
    j2 = p.push_job(TEST_SPIDER_NAME, testid=1)
    j3 = p.push_job(TEST_SPIDER_NAME, testid=2)
    # global list must list at least one job
    assert list(p.get_jobs(count=1, state='pending'))
    # List all jobs for test spider
    r = list(p.get_jobs(spider=TEST_SPIDER_NAME, state='pending'))
    assert [j.key for j in r] == [j3.key, j2.key, j1.key]


def test_get_jobs_with_legacy_filter(hsproject):
    p = hsproject
    j1 = p.push_job(TEST_SPIDER_NAME, state='finished',
                    close_reason='finished', tags=['t2'])
    j2 = p.push_job(TEST_SPIDER_NAME, state='finished',
                    close_reason='finished', tags=['t1'])
    j3 = p.push_job(TEST_SPIDER_NAME, state='pending')
    j4 = p.push_job(TEST_SPIDER_NAME, state='finished',
                    close_reason='failed', tags=['t1'])
    j5 = p.push_job(TEST_SPIDER_NAME + 'skip', state='finished',
                    close_reason='failed', tags=['t1'])
    filters = [
        ['spider', '=', [TEST_SPIDER_NAME]],
        ['state', '=', ['finished']],
        ['close_reason', '=', ['finished']],
        ['tags', 'haselement', ['t1']],
        ['tags', 'hasnotelement', ['t2']],
    ]
    jobs = p.get_jobs(filter=[json.dumps(x) for x in filters])
    assert [j.key for j in jobs] == [j2.key], jobs


def test_push_job(hsproject):
    job = hsproject.push_job(TEST_SPIDER_NAME, state='running',
                             priority=hsproject.jobq.PRIO_HIGH,
                             foo='bar')
    assert job.metadata.get('state') == 'running'
    assert job.metadata.get('foo') == 'bar'
    hsproject.jobq.finish(job)
    hsproject.jobq.delete(job)
    job.metadata.expire()
    assert job.metadata.get('state') == 'deleted'
    assert job.metadata.get('foo') == 'bar'


def test_auth(hsclient, json_and_msgpack):
    # client without global auth set
    hsc = HubstorageClient(endpoint=hsclient.endpoint,
                           use_msgpack=hsclient.use_msgpack)
    assert hsc.auth is None

    # check no-auth access
    try:
        hsc.push_job(TEST_PROJECT_ID, TEST_SPIDER_NAME)
    except HTTPError as exc:
        assert exc.response.status_code == 401
    else:
        raise AssertionError('401 not raised')

    try:
        hsc.get_project(TEST_PROJECT_ID).push_job(TEST_SPIDER_NAME)
    except HTTPError as exc:
        assert exc.response.status_code == 401
    else:
        raise AssertionError('401 not raised')

    try:
        hsc.get_job((TEST_PROJECT_ID, 1, 1)).items.list()
    except HTTPError as exc:
        assert exc.response.status_code == 401
    else:
        raise AssertionError('401 not raised')

    try:
        hsc.get_project(TEST_PROJECT_ID).get_job(
            (TEST_PROJECT_ID, 1, 1)).items.list()
    except HTTPError as exc:
        assert exc.response.status_code == 401
    else:
        raise AssertionError('401 not raised')

    # create project with auth
    auth = hsclient.auth
    project = hsc.get_project(TEST_PROJECT_ID, auth)
    assert project.auth == auth
    job = project.push_job(TEST_SPIDER_NAME)
    samejob = project.get_job(job.key)
    assert samejob.key == job.key


def test_broad(hsproject, hsspiderid, json_and_msgpack):
    # populate project with at least one job
    job = hsproject.push_job(TEST_SPIDER_NAME)
    assert job.metadata.get('state') == 'pending'
    job = start_job(hsproject)
    job.metadata.expire()
    assert job.metadata.get('state') == 'running'
    job.items.write({'title': 'bar'})
    job.logs.info('nice to meet you')
    job.samples.write([1, 2, 3])
    job.close_writers()
    job.jobq.finish(job)

    # keep a jobid for get_job and unreference job
    jobid = job.key
    jobauth = job.auth
    del job

    assert list(hsproject.items.list(hsspiderid, count=1))
    assert list(hsproject.logs.list(hsspiderid, count=1))
    assert list(hsproject.samples.list(hsspiderid, count=1))

    job = hsproject.client.get_job(jobid, jobauth=jobauth)
    job.purged()


@pytest.fixture
def unset_botgroup(hsproject):
    unset_testbotgroup(hsproject)
    yield
    set_testbotgroup(hsproject)


def test_settings(hsproject, unset_botgroup):
    settings = dict(hsproject.settings)
    assert settings == {}
    # use some fixed timestamp to represent current time
    hsproject.settings['created'] = created = 1476803148638
    hsproject.settings['botgroups'] = ['g1']
    hsproject.settings.save()
    assert hsproject.settings.liveget('created') == created
    assert hsproject.settings.liveget('botgroups') == ['g1']
    hsproject.settings.expire()
    assert dict(hsproject.settings) == {
        'created': created,
        'botgroups': ['g1'],
    }


def test_requests(hsproject):
    # use some fixed timestamp to represent current time
    ts = 1476803148638
    job = hsproject.push_job(TEST_SPIDER_NAME, state='running')
    # top parent
    r1 = job.requests.add(url='http://test.com/', status=200, method='GET',
                          rs=1337, duration=5, parent=None, ts=ts)
    # first child
    r2 = job.requests.add(url='http://test.com/2', status=400, method='POST',
                          rs=0, duration=1, parent=r1, ts=ts + 1)
    # another child with fingerprint set
    r3 = job.requests.add(url='http://test.com/3', status=400, method='PUT',
                          rs=0, duration=1, parent=r1, ts=ts + 2, fp='1234')

    job.requests.close()
    rr = job.requests.list()
    assert next(rr) == {
        'status': 200, 'rs': 1337,
        'url': 'http://test.com/', 'time': ts,
        'duration': 5, 'method': 'GET',
    }
    assert next(rr) == {
        'status': 400, 'parent': 0, 'rs': 0,
        'url': 'http://test.com/2', 'time': ts + 1,
        'duration': 1, 'method': 'POST',
    }
    assert next(rr) == {
        'status': 400, 'fp': '1234', 'parent': 0,
        'rs': 0, 'url': 'http://test.com/3',
        'time': ts + 2, 'duration': 1,
        'method': 'PUT',
    }
    with pytest.raises(StopIteration):
        next(rr)


def test_samples(hsproject, json_and_msgpack):
    # use some fixed timestamp to represent current time
    ts = 1476803148638
    # no samples stored
    j1 = hsproject.push_job(TEST_SPIDER_NAME, state='running')
    assert list(j1.samples.list()) == []
    # simple fill
    j1.samples.write([ts, 1, 2, 3])
    j1.samples.write([ts + 1, 5, 9, 4])
    j1.samples.flush()
    o = list(j1.samples.list())
    assert len(o) == 2
    assert o[0] == [ts, 1, 2, 3]
    assert o[1] == [ts + 1, 5, 9, 4]

    # random fill
    j2 = hsproject.push_job(TEST_SPIDER_NAME, state='running')
    samples = []
    count = int(j2.samples.batch_size * 3)
    for i in range(count):
        ts += i
        row = [ts] + list(val*i for val in range(10))
        samples.append(row)
        j2.samples.write(row)
    j2.samples.flush()
    o = list(j2.samples.list())
    assert len(o) == count
    for r1, r2 in zip(samples, o):
        assert r1 == r2


def test_jobsummary(hsproject):
    js = hsproject.jobsummary()
    assert js.get('project') == int(hsproject.projectid), js
    assert js.get('has_capacity') is True, js
    assert 'pending' in js, js
    assert 'running' in js, js


def test_bulkdata(hsproject, json_and_msgpack):
    j = hsproject.push_job(TEST_SPIDER_NAME, state='running')
    for i in range(20):
        j.logs.info("log line %d" % i)
        j.items.write(dict(field1="item%d" % i))
        j.requests.add("http://test.com/%d" % i, 200, 'GET', 10, None, 10, 120)
    for resourcename in ('logs', 'items', 'requests'):
        resource = getattr(j, resourcename)
        resource.flush()

        # downloading resource, with simulated failures
        with failing_downloader(resource):
            downloaded = list(resource.iter_values())
            assert len(downloaded) == 20


def test_output_string(hsclient, hsproject):
    hsproject.push_job(TEST_SPIDER_NAME)
    job = start_job(hsproject)
    job.items.write({'foo': 'bar'})
    job.close_writers()
    items = hsclient.get_job(job.key).items.iter_json()
    assert isinstance(next(items), str)


@pytest.mark.parametrize('path,expected_result', [
    (None, True),
    ('33/1', True),
    ('33/1/', True),
    ((33, 1), True),
    ('stats', False),
    ('stats/', False),
    ('33/1/stats', False),
    ('33/1/stats/', False),
    ((33, 1, 'stats'), False),
])
def test_allows_msgpack(hsclient, path, expected_result, json_and_msgpack):
    job = hsclient.get_job('2222000/1/1')
    for resource in [job.items, job.logs, job.samples]:
        assert resource._allows_mpack(path) is (hsclient.use_msgpack and expected_result)
    assert job.requests._allows_mpack(path) is False
    assert job.metadata._allows_mpack(path) is False
    assert job.jobq._allows_mpack(path) is False
