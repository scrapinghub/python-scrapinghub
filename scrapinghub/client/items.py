from __future__ import absolute_import

from .proxy import _ItemsResourceProxy, _DownloadableProxyMixin


class Items(_DownloadableProxyMixin, _ItemsResourceProxy):
    """Representation of collection of job items.

    Not a public constructor: use :class:`~scrapinghub.client.jobs.Job`
    instance to get a :class:`Items` instance. See
    :attr:`~scrapinghub.client.jobs.Job.items` attribute.

    Please note that :meth:`list` method can use a lot of memory and for
    a large number of items it's recommended to iterate through them via
    :meth:`iter` method (all params and available filters are same for
    both methods).

    Usage:

    - retrieve all scraped items from a job::

        >>> job.items.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - iterate through first 100 items and print them::

        >>> for item in job.items.iter(count=100):
        ...     print(item)

    - retrieve items with timestamp greater or equal to given timestamp
      (item here is an arbitrary dictionary depending on your code)::

        >>> job.items.list(startts=1447221694537)
        [{
            'name': ['Some custom item'],
            'url': 'http://some-url/item.html',
            'size': 100000,
        }]

    - retrieve 1 item with multiple filters::

        >>> filters = [("size", ">", [30000]), ("size", "<", [40000])]
        >>> job.items.list(count=1, filter=filters)
        [{
            'name': ['Some other item'],
            'url': 'http://some-url/other-item.html',
            'size': 35000,
        }]
    """

    def _modify_iter_params(self, params):
        """Modify iter filter to convert offset to start parameter.

        :return: a dict with updated set of params.
        :rtype: :class:`dict`
        """
        params = super(Items, self)._modify_iter_params(params)
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '{}/{}'.format(self.key, offset)
        return params
