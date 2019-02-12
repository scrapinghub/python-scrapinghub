"""
Test job metadata

System tests for operations on stored job metadata
"""
from ..conftest import TEST_SPIDER_NAME
from .conftest import start_job


def _assertMetadata(meta1, meta2):
    def _clean(m):
        return dict((k, v) for k, v in m.items() if k != 'updated_time')

    meta1 = _clean(meta1)
    meta2 = _clean(meta2)
    assert meta1 == meta2


def test_basic(hsclient, hsproject):
    job = hsproject.push_job(TEST_SPIDER_NAME)
    assert 'auth' not in job.metadata
    assert 'state' in job.metadata
    assert job.metadata['spider'] == TEST_SPIDER_NAME

    # set some metadata and forget it
    job.metadata['foo'] = 'bar'
    assert job.metadata['foo'] == 'bar'
    job.metadata.expire()
    assert 'foo' not in job.metadata

    # set it again and persist it
    job.metadata['foo'] = 'bar'
    assert job.metadata['foo'] == 'bar'
    job.metadata.save()
    assert job.metadata['foo'] == 'bar'
    job.metadata.expire()
    assert job.metadata['foo'] == 'bar'

    # refetch the job and compare its metadata
    job2 = hsclient.get_job(job.key)
    _assertMetadata(job2.metadata, job.metadata)

    # delete foo but do not persist it
    del job.metadata['foo']
    assert 'foo' not in job.metadata
    job.metadata.expire()
    assert job.metadata.get('foo') == 'bar'
    # persist it to be sure it is not removed
    job.metadata.save()
    assert job.metadata.get('foo') == 'bar'
    # and finally delete again and persist it
    del job.metadata['foo']
    assert 'foo' not in job.metadata
    job.metadata.save()
    assert 'foo' not in job.metadata
    job.metadata.expire()
    assert 'foo' not in job.metadata

    job2 = hsclient.get_job(job.key)
    _assertMetadata(job.metadata, job2.metadata)


def test_updating(hsproject):
    job = hsproject.push_job(TEST_SPIDER_NAME)
    assert job.metadata.get('foo') is None
    job.update_metadata({'foo': 'bar'})
    # metadata attr should change
    assert job.metadata.get('foo') == 'bar'
    # as well as actual metadata
    job = hsproject.get_job(job.key)
    assert job.metadata.get('foo') == 'bar'
    job.update_metadata({'foo': None})
    assert not job.metadata.get('foo', False)

    # there are ignored fields like: auth, _key, state
    state = job.metadata['state']
    job.update_metadata({'state': 'running'})
    assert job.metadata['state'] == state


def test_representation(hsproject):
    job = hsproject.push_job(TEST_SPIDER_NAME)
    meta = job.metadata
    assert str(meta) != repr(meta)
    assert meta == eval(str(meta))
    assert meta.__class__.__name__ in repr(meta)
    assert meta.__class__.__name__ not in str(meta)


def test_jobauth(hsclient, hsproject):
    job = hsproject.push_job(TEST_SPIDER_NAME)
    assert job.jobauth is None
    assert job.auth == hsproject.auth
    assert job.items.auth == hsproject.auth

    samejob = hsclient.get_job(job.key)
    assert samejob.auth is None
    assert samejob.jobauth is None
    assert samejob.items.auth == hsproject.auth


def test_authtoken(hsproject):
    pendingjob = hsproject.push_job(TEST_SPIDER_NAME)
    runningjob = start_job(hsproject)
    assert pendingjob.key == runningjob.key
    assert runningjob.jobauth
    assert runningjob.jobauth == runningjob.auth
    assert runningjob.auth[0] == runningjob.key
    assert runningjob.auth[1]
