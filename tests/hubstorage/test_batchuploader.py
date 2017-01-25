"""
Test Project
"""
import time
import pytest
from six.moves import range
from collections import defaultdict

from scrapinghub.hubstorage import ValueTooLarge
from .conftest import TEST_SPIDER_NAME, TEST_AUTH
from .conftest import start_job


def _job_and_writer(hsclient, hsproject, **writerargs):
    hsproject.push_job(TEST_SPIDER_NAME)
    job = start_job(hsproject)
    batch_uploader = hsclient.batchuploader
    writer = batch_uploader.create_writer(
        job.items.url, auth=TEST_AUTH, **writerargs)
    return job, writer


def test_writer_batchsize(hsclient, hsproject):
    job, writer = _job_and_writer(hsclient, hsproject, size=10)
    for x in range(111):
        writer.write({'x': x})
    writer.close()
    # this works only for small batches (previous size=10 and small data)
    # as internally HS may commit a single large request as many smaller
    # commits, each with different timestamps
    groups = defaultdict(int)
    for doc in job.items.list(meta=['_ts']):
        groups[doc['_ts']] += 1

    assert len(groups) == 12


def test_writer_maxitemsize(hsclient, hsproject):
    _, writer = _job_and_writer(hsclient, hsproject)
    max_size = writer.maxitemsize
    with pytest.raises(ValueTooLarge) as excinfo1:
        writer.write({'b': 'x' * max_size})
    excinfo1.match(
        r'Value exceeds max encoded size of 1 MiB:'
        ' \'{"b": "x+\\.\\.\\.\'')

    with pytest.raises(ValueTooLarge) as excinfo2:
        writer.write({'b'*max_size: 'x'})
    excinfo2.match(
        r'Value exceeds max encoded size of 1 MiB:'
        ' \'{"b+\\.\\.\\.\'')

    with pytest.raises(ValueTooLarge) as excinfo3:
        writer.write({'b'*(max_size//2): 'x'*(max_size//2)})
    excinfo3.match(
        r'Value exceeds max encoded size of 1 MiB:'
        ' \'{"b+\\.\\.\\.\'')


def test_writer_maxitemsize_custom(hsclient, hsproject):
    _, writer = _job_and_writer(hsclient, hsproject, maxitemsize=512*1024)
    with pytest.raises(ValueTooLarge) as excinfo:
        writer.write({'b': 'x' * writer.maxitemsize})
    excinfo.match(
        r'Value exceeds max encoded size of 512 KiB:'
        ' \'{"b": "x+\\.\\.\\.\'')


def test_writer_contentencoding(hsclient, hsproject):
    for ce in ('identity', 'gzip'):
        job, writer = _job_and_writer(hsclient, hsproject,
                                      content_encoding=ce)
        for x in range(111):
            writer.write({'x': x})
        writer.close()
        assert job.items.stats()['totals']['input_values'] == 111


def test_writer_interval(hsclient, hsproject):
    job, writer = _job_and_writer(hsclient, hsproject,
                                  size=1000, interval=1)
    for x in range(111):
        writer.write({'x': x})
        if x == 50:
            time.sleep(2)

    writer.close()
    groups = defaultdict(int)
    for doc in job.items.list(meta=['_ts']):
        groups[doc['_ts']] += 1

    assert len(groups) == 2
