import types

import pytest

from .conftest import TEST_PROJECT_ID


def _add_test_activity(project):
    activity = project.activity
    jobkey = TEST_PROJECT_ID + '/2/3'
    events = [{'event': 'job:completed', 'job': jobkey, 'user': 'jobrunner'},
              {'event': 'job:cancelled', 'job': jobkey, 'user': 'john'}]
    activity.add(events)


def test_activity_wrong_project(project):
    event = {'event': 'job:completed', 'job': '123/1/1', 'user': 'user'}
    with pytest.raises(ValueError):
        project.activity.add(event)


def test_activity_iter(project):
    _add_test_activity(project)
    activity = project.activity.iter()
    assert isinstance(activity, types.GeneratorType)
    activity_item = next(activity)
    assert activity_item == {'event': 'job:cancelled',
                             'job': TEST_PROJECT_ID + '/2/3',
                             'user': 'john'}


def test_activity_list(project):
    _add_test_activity(project)
    activity = project.activity.list(count=2)
    assert isinstance(activity, list)
    assert len(activity) == 2
    assert activity[0] == {'event': 'job:cancelled',
                           'job': TEST_PROJECT_ID + '/2/3',
                           'user': 'john'}
    assert activity[1] == {'event': 'job:completed',
                           'job': TEST_PROJECT_ID + '/2/3',
                           'user': 'jobrunner'}
