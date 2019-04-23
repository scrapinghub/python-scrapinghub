from __future__ import absolute_import

from .proxy import _ItemsResourceProxy, _DownloadableProxyMixin


class Requests(_DownloadableProxyMixin, _ItemsResourceProxy):
    """Representation of collection of job requests.

    Not a public constructor: use :class:`~scrapinghub.client.jobs.Job` instance
    to get a :class:`Requests` instance.
    See :attr:`~scrapinghub.client.jobs.Job.requests` attribute.

    Please note that :meth:`list` method can use a lot of memory and for
    a large amount of logs it's recommended to iterate through it via
    :meth:`iter` method (all params and available filters are same for
    both methods).

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
    def add(self, url, status, method, rs, duration, ts, parent=None, fp=None):
        """ Add a new requests.

        :param url: string url for the request.
        :param status: HTTP status of the request.
        :param method: stringified request method.
        :param rs: response body length.
        :param duration: request duration in milliseconds.
        :param ts: UNIX timestamp in milliseconds.
        :param parent: (optional) parent request id.
        :param fp: (optional) string fingerprint for the request.
        """
        return self._origin.add(
            url, status, method, rs, parent, duration, ts, fp=fp)
