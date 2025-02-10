import time
from types import GeneratorType

from collections.abc import Iterable

from scrapinghub.client.frontiers import Frontiers, Frontier, FrontierSlot
from ..conftest import TEST_FRONTIER_SLOT


def _add_test_requests_to_frontier(frontier):
    slot = frontier.get(TEST_FRONTIER_SLOT)
    slot.q.add([{'fp': '/some/path.html'}, {'fp': '/other/path.html'}])
    slot.flush()


def _clean_project_frontiers(project):
    """Helper to clean slots of all frontiers for a project.

    frontier fixture cleans a test slot before each test, but for some tests
    it's convenient to clean all frontiers and test with 0 counters.
    """
    for frontier_name in project.frontiers.iter():
        frontier = project.frontiers.get(frontier_name)
        for slot_name in frontier.iter():
            frontier.get(slot_name).delete()


def test_frontiers(project, frontier, frontier_name):
    # reset a test slot and add some requests to init it
    frontier.get(TEST_FRONTIER_SLOT).delete()
    _add_test_requests_to_frontier(frontier)

    assert isinstance(project.frontiers, Frontiers)
    frontiers = project.frontiers

    # test for iter() method
    frontiers_names = frontiers.iter()
    assert isinstance(frontiers_names, Iterable)
    assert frontier_name in list(frontiers_names)

    # test for list() method
    frontiers_names = frontiers.list()
    assert frontier_name in frontiers_names

    # test for get() method
    frontier = frontiers.get(frontier_name)
    assert isinstance(frontier, Frontier)

    # other tests
    frontiers.flush()
    assert isinstance(frontiers.newcount, int)


def test_frontier(project, frontier):
    # add some requests to test frontier to init a test slot
    frontier.get(TEST_FRONTIER_SLOT).delete()
    _add_test_requests_to_frontier(frontier)

    slots = frontier.iter()
    assert isinstance(slots, Iterable)
    assert TEST_FRONTIER_SLOT in list(slots)

    slots = frontier.list()
    assert TEST_FRONTIER_SLOT in slots

    slot = frontier.get(TEST_FRONTIER_SLOT)
    assert isinstance(slot, FrontierSlot)

    frontier.flush()


def test_frontier_slot(project, frontier):
    # add some requests to test frontier to init a test slot
    frontier.get(TEST_FRONTIER_SLOT).delete()
    _add_test_requests_to_frontier(frontier)

    slot = frontier.get(TEST_FRONTIER_SLOT)

    # get all batches from slot and validate its content
    batches_iter = slot.q.iter()
    assert isinstance(batches_iter, GeneratorType)
    batches = list(batches_iter)
    assert len(batches) == 1
    assert isinstance(batches[0], dict)
    assert sorted(batches[0].keys()) == ['id', 'requests']
    assert isinstance(batches[0]['id'], str)
    requests = batches[0]['requests']
    assert len(requests) == 2
    assert requests == [['/some/path.html', None],
                        ['/other/path.html', None]]

    # validate that slot.list() returns same data as slot.q.iter()
    batches_list = slot.q.list()
    assert isinstance(batches, list)
    assert batches_list == batches

    # add a requests with additional parameters
    slot.q.add([{'fp': 'page1.html', 'p': 1, 'qdata': {'depth': 1}}])
    slot.flush()
    batches = slot.q.list()
    assert len(batches) == 2
    assert batches[1]['requests'] == [['page1.html', {'depth': 1}]]

    # drop all batches and validate that slot is empty
    slot.q.delete([batch['id'] for batch in batches])
    assert slot.q.list() == []

    slot.delete()
    assert TEST_FRONTIER_SLOT not in frontier.list()


def test_frontier_newcount(project, frontier):
    _clean_project_frontiers(project)
    first_slot = frontier.get(TEST_FRONTIER_SLOT)

    assert frontier._frontiers.newcount == 0
    assert frontier.newcount == 0
    assert first_slot.newcount == 0

    # shorter batch interval for faster tests
    frontier._frontiers._origin.batch_interval = 0.1
    _add_test_requests_to_frontier(frontier)
    time.sleep(0.5)

    assert frontier._frontiers.newcount == 2
    assert frontier.newcount == 2
    assert first_slot.newcount == 2

    second_slot = frontier.get('test2.com')
    second_slot.delete()
    second_slot.q.add([{'fp': '/different_path.html'}])
    second_slot.flush()

    assert frontier._frontiers.newcount == 3
    assert frontier.newcount == 3
    assert second_slot.newcount == 1
    assert first_slot.newcount == 2

    frontier._frontiers.close()
