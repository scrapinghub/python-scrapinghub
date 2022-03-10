from __future__ import absolute_import

import sys

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

    - retrieve items via a generator of lists. This is most useful in cases
      where the job has a huge amount of items and it needs to be broken down
      into chunks when consumed. This example shows a job with 3 items::

        >>> gen = job.items.list_iter(chunksize=2)
        >>> next(gen)
        [{'name': 'Item #1'}, {'name': 'Item #2'}]
        >>> next(gen)
        [{'name': 'Item #3'}]
        >>> next(gen)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

    - retrieving via meth::`list_iter` also supports the `start` and `count`.
      params. This is useful when you want to only retrieve a subset of items in
      a job. The example below belongs to a job with 10 items::

        >>> gen = job.items.list_iter(chunksize=2, start=5, count=3)
        >>> next(gen)
        [{'name': 'Item #5'}, {'name': 'Item #6'}]
        >>> next(gen)
        [{'name': 'Item #7'}]
        >>> next(gen)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

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

    def list_iter(self, chunksize=1000, *args, **kwargs):
        """An alternative interface for reading items by returning them
        as a generator which yields lists of items sized as `chunksize`.

        This is a convenient method for cases when processing a large amount of
        items from a job isn't ideal in one go due to the large memory needed.
        Instead, this allows you to process it chunk by chunk.

        You can improve I/O overheads by increasing the chunk value but that
        would also increase the memory consumption.

        :param chunksize: size of list to be returned per iteration
        :param start: offset to specify the start of the item iteration
        :param count: overall number of items to be returned, which is broken
            down by `chunksize`.

        :return: an iterator over items, yielding lists of items.
        :rtype: :class:`collections.abc.Iterable`
        """

        start = kwargs.pop("start", 0)
        count = kwargs.pop("count", sys.maxsize)
        processed = 0

        while True:
            next_key = self.key + "/" + str(start)
            if processed + chunksize > count:
                chunksize = count - processed
            items = [
                item for item in self.iter(
                    count=chunksize, start=next_key, *args, **kwargs)
            ]
            yield items
            processed += len(items)
            start += len(items)
            if processed >= count:
                break
            if len(items) < chunksize:
                break
