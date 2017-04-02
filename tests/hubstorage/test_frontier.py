"""
Test Frontier
"""
import pytest

from .conftest import TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT


@pytest.fixture(autouse=True)
def delete_frontier_slot(hsproject):
    frontier = hsproject.frontier
    frontier.delete_slot(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT)


def _get_urls(batch):
    return [r[0] for r in batch['requests']]


def test_add_read(hsproject):
    frontier = hsproject.frontier

    fps = [{'fp': '/'}]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps)
    fps = [{'fp': '/index.html'}, {'fp': '/index2.html'}]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps)
    frontier.flush()

    urls = [_get_urls(batch) for batch
            in frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT)]
    expected_urls = [[u'/', u'/index.html', u'/index2.html']]
    assert urls == expected_urls


def test_add_multiple_chunks(hsproject):
    frontier = hsproject.frontier
    old_count = frontier.newcount
    initial_count = frontier.count_slot(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT)

    batch_size = 50
    fps1 = [{'fp': '/index_%s.html' % fp} for fp in range(0, batch_size)]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps1)

    fps2 = [{'fp': '/index_%s.html' % fp}
            for fp in range(batch_size, batch_size * 2)]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps2)

    fps3 = [{'fp': '/index_%s.html' % fp}
            for fp in range(batch_size * 2, batch_size * 3)]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps3)
    frontier.flush()

    assert frontier.newcount == 150 + old_count

    # test count_slot
    count = frontier.count_slot(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT)
    assert count['count'] == initial_count['count'] + 150

    # insert repeated fingerprints
    fps4 = [{'fp': '/index_%s.html' % fp} for fp in range(0, batch_size)]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps3)
    frontier.flush()

    # new count is the same
    assert frontier.newcount == 150 + old_count

    # get first 100
    batches = list(frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT,
                                 mincount=100))
    urls = [_get_urls(batch) for batch in batches]
    expected_urls = [[fp['fp'] for fp in fps1 + fps2]]
    assert urls == expected_urls

    # delete first 100
    ids = [batch['id'] for batch in batches]
    frontier.delete(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, ids)

    # test count_slot again
    count = frontier.count_slot(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT)
    assert count['count'] - initial_count['count'] == 150 - 100

    # get remaining 50
    batches = list(frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT))
    urls = [_get_urls(batch) for batch in batches]
    expected_urls = [[fp['fp'] for fp in fps3]]
    assert urls == expected_urls


def test_add_big_chunk(hsproject):
    frontier = hsproject.frontier

    batch_size = 300
    fps1 = [{'fp': '/index_%s.html' % fp} for fp in range(0, batch_size)]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps1)
    frontier.flush()

    # get first 100
    batches = list(frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT,
                                 mincount=100))
    urls = [_get_urls(batch) for batch in batches]
    expected_urls = [[fp['fp'] for fp in fps1[:100]]]
    assert urls == expected_urls

    # delete first 100
    ids = [batch['id'] for batch in batches]
    frontier.delete(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, ids)

    # get next 100
    batches = list(frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT,
                                 mincount=100))
    urls = [_get_urls(batch) for batch in batches]
    expected_urls = [[fp['fp'] for fp in fps1[100:200]]]
    assert urls == expected_urls

    # delete next 100
    ids = [batch['id'] for batch in batches]
    frontier.delete(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, ids)

    # get next 100
    batches = list(frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT,
                                 mincount=100))
    urls = [_get_urls(batch) for batch in batches]
    expected_urls = [[fp['fp'] for fp in fps1[200:300]]]
    assert urls == expected_urls


def test_add_extra_params(hsproject):
    frontier = hsproject.frontier

    qdata = {"a": 1, "b": 2, "c": 3}
    fps = [{'fp': '/', "qdata": qdata}]
    frontier.add(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT, fps)
    frontier.flush()

    expected_request = [[u'/', {u'a': 1, u'c': 3, u'b': 2}]]
    batches = list(frontier.read(TEST_FRONTIER_NAME, TEST_FRONTIER_SLOT))
    request = batches[0]['requests']
    assert request == expected_request
