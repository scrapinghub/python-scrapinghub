from __future__ import absolute_import

from .utils import _Proxy
from .utils import parse_job_key


class Activity(_Proxy):
    """Representation of collection of job activity events.

    Not a public constructor: use :class:`Project` instance to get a
    :class:`Activity` instance. See :attr:`Project.activity` attribute.

    Please note that list() method can use a lot of memory and for a large
    amount of activities it's recommended to iterate through it via iter()
    method (all params and available filters are same for both methods).

    Usage:

    - get all activity from a project::

        >>> project.activity.iter()
        <generator object jldecode at 0x1049ee990>

    - get only last 2 events from a project::

        >>> project.activity.list(count=2)
        [{'event': 'job:completed', 'job': '123/2/3', 'user': 'jobrunner'},
         {'event': 'job:started', 'job': '123/2/3', 'user': 'john'}]

    - post a new event::

        >>> event = {'event': 'job:completed',
                     'job': '123/2/4',
                     'user': 'jobrunner'}
        >>> project.activity.add(event)

    - post multiple events at once::

        >>> events = [
            {'event': 'job:completed', 'job': '123/2/5', 'user': 'jobrunner'},
            {'event': 'job:cancelled', 'job': '123/2/6', 'user': 'john'},
        ]
        >>> project.activity.add(events)

    """
    def __init__(self, *args, **kwargs):
        super(Activity, self).__init__(*args, **kwargs)
        self._proxy_methods([('iter', 'list')])
        self._wrap_iter_methods(['iter'])

    def add(self, values, **kwargs):
        """Add new event to the project activity.

        :param values: a single event or a list of events, where event is
            represented with a dictionary of ('event', 'job', 'user') keys.
        """
        if not isinstance(values, list):
            values = list(values)
        for activity in values:
            if not isinstance(activity, dict):
                raise ValueError("Please pass events as dictionaries")
            job_key = activity.get('job')
            if job_key and parse_job_key(job_key).project_id != self.key:
                raise ValueError('Please use same project id')
        self._origin.post(values, **kwargs)
