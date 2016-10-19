import random
from contextlib import closing

import pytest
from six.moves import range

from scrapinghub import HubstorageClient
from scrapinghub.hubstorage.utils import millitime

from .conftest import TEST_ENDPOINT, TEST_SPIDER_NAME
from .conftest import TEST_PROJECT_ID, TEST_AUTH
from .conftest import start_job


MAGICN = 1211


@pytest.fixture
def panelclient():
    # Panel - no client auth, only project auth using user auth token
    return HubstorageClient(endpoint=TEST_ENDPOINT)


@pytest.fixture
def panelproject(panelclient):
    return panelclient.get_project(TEST_PROJECT_ID, auth=TEST_AUTH)


@pytest.fixture(autouse=True)
def close_panelclient(panelclient):
    yield
    panelclient.close()


def test_succeed_with_close_reason(hsproject, panelproject):
    _do_test_success(hsproject, panelproject, 'all-good', 'all-good')


def test_succeed_without_close_reason(hsproject, panelproject):
    _do_test_success(hsproject, panelproject, None, 'no_reason')


def _do_test_success(*args):
    """Simple wrapper around _do_test_job with additonal checks"""
    job = _do_test_job(*args)
    assert job.items.stats()['totals']['input_values'] == MAGICN
    assert job.logs.stats()['totals']['input_values'] == MAGICN * 4
    assert job.requests.stats()['totals']['input_values'] == MAGICN


def test_scraper_failure(hsproject, panelproject):
    job = _do_test_job(
        hsproject,
        panelproject,
        IOError('no more resources, ha!'),
        'failed',
    )
    # MAGICN per log level messages plus one of last failure
    stats = job.logs.stats()
    assert stats
    assert stats['totals']['input_values'] == MAGICN * 4 + 1


def _do_test_job(hsproject, panelproject,
                 job_close_reason, expected_close_reason):
    pushed = panelproject.jobq.push(TEST_SPIDER_NAME)
    # check pending state
    job = panelproject.get_job(pushed['key'])
    assert job.metadata.get('state') == 'pending'
    # consume msg from runner
    _run_runner(hsproject, pushed, close_reason=job_close_reason)
    # query again from panel
    job = panelproject.get_job(pushed['key'])
    assert job.metadata.get('state') == 'finished'
    assert job.metadata.get('close_reason') == expected_close_reason
    return job

def _run_runner(hsproject, pushed, close_reason):
    client = HubstorageClient(endpoint=TEST_ENDPOINT, auth=TEST_AUTH)
    with closing(client) as runnerclient:
        job = start_job(hsproject)
        assert not job.metadata.get('stop_requested')
        job.metadata.update(host='localhost', slot=1)
        assert job.metadata.get('state') == 'running'
        # run scraper
        try:
            _run_scraper(job.key, job.jobauth, close_reason=close_reason)
        except Exception as exc:
            job.logs.error(message=str(exc), appendmode=True)
            job.close_writers()
            job.jobq.finish(job, close_reason='failed')
            # logging from runner must append and never remove messages logged
            # by scraper
            assert job.logs.batch_append
        else:
            job.jobq.finish(job, close_reason=close_reason or 'no_reason')


def _run_scraper(jobkey, jobauth, close_reason=None):
    httpmethods = 'GET PUT POST DELETE HEAD OPTIONS TRACE CONNECT'.split()
    # Scraper - uses job level auth, no global or project auth available
    client = HubstorageClient(endpoint=TEST_ENDPOINT)
    # use some fixed timestamp to represent current time
    now_ts = 1476803148638
    with closing(client) as scraperclient:
        job = scraperclient.get_job(jobkey, auth=jobauth)
        for idx in range(MAGICN):
            iid = job.items.write({'uuid': idx})
            job.logs.debug('log debug %s' % idx, idx=idx)
            job.logs.info('log info %s' % idx, idx=idx)
            job.logs.warn('log warn %s' % idx, idx=idx)
            job.logs.error('log error %s' % idx, idx=idx)
            sid = job.samples.write([idx, idx, idx])
            rid = job.requests.add(
                url='http://test.com/%d' % idx,
                status=random.randint(100, 1000),
                method=random.choice(httpmethods),
                rs=random.randint(0, 100000),
                duration=random.randint(0, 1000),
                parent=random.randrange(0, idx + 1) if idx > 10 else None,
                ts=now_ts + 100 + idx,
            )
            assert iid == idx
            assert sid == idx
            assert rid == idx

        if isinstance(close_reason, Exception):
            raise close_reason

        if close_reason:
            job.metadata['close_reason'] = close_reason

        job.metadata.save()
