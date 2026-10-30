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

    Besides the parameters of the :ref:`zyte:api-items`, :meth:`iter`
    supports a *filter* parameter: a list of ``(field, operator, values)``
    tuples, all of which an item must match, as shown in the last example
    below.

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
        as a generator which yields lists of up to *chunksize* items.

        This is a convenient method for cases when processing a large amount of
        items from a job isn't ideal in one go due to the large memory needed.
        Instead, this allows you to process it chunk by chunk.

        You can improve I/O overheads by increasing the chunk value but that
        would also increase the memory consumption.

        *start* is the index of the first item to read, and *count* the
        overall number of items to return. Other parameters, e.g. *filter*,
        work as in :meth:`iter`.

        :return: an iterator over items, yielding lists of items.
        :rtype: :class:`collections.abc.Iterable`
        """

        start = kwargs.pop("start", 0)
        count = kwargs.pop("count", sys.maxsize)
        meta = kwargs.pop("meta", None) or []
        if isinstance(meta, str):
            meta = [meta]
        drop_key = "_key" not in meta
        if drop_key:
            meta = [*meta, "_key"]
        processed = 0

        while True:
            next_key = self.key + "/" + str(start)
            if processed + chunksize > count:
                chunksize = count - processed
            items = [
                item for item in self.iter(
                    count=chunksize, start=next_key, meta=meta,
                    *args, **kwargs)
            ]
            if items:
                # Filters may skip items, so resume right after the last
                # item returned.
                start = int(items[-1]["_key"].rsplit("/", 1)[1]) + 1
            if drop_key:
                for item in items:
                    del item["_key"]
            yield items
            processed += len(items)
            if processed >= count:
                break
            if len(items) < chunksize:
                break
