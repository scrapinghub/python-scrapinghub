from __future__ import absolute_import

from .utils import _Proxy


class Requests(_Proxy):
    """Representation of collection of job requests.

    Not a public constructor: use :class:`Job` instance to get a
    :class:`Requests` instance. See :attr:`Job.requests` attribute.

    Please note that list() method can use a lot of memory and for a large
    amount of requests it's recommended to iterate through it via iter()
    method (all params and available filters are same for both methods).

    Usage:

    - retrieve all requests from a job::

        >>> job.requests.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - iterate through the requests::

        >>> for reqitem in job.requests.iter(count=1):
        ...     print(reqitem['time'])
        1482233733870

    - retrieve single request from a job::

        >>> job.requests.list(count=1)
        [{
            'duration': 354,
            'fp': '6d748741a927b10454c83ac285b002cd239964ea',
            'method': 'GET',
            'rs': 1270,
            'status': 200,a
            'time': 1482233733870,
            'url': 'https://example.com'
        }]
    """
    def __init__(self, *args, **kwargs):
        super(Requests, self).__init__(*args, **kwargs)
        self._proxy_methods(['add'])
