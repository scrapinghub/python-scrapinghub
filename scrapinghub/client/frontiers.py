from __future__ import absolute_import
from functools import partial
from collections import defaultdict

from six import string_types

from ..hubstorage.frontier import Frontier as _Frontier
from ..hubstorage.utils import urlpathjoin

from .proxy import _Proxy
from .utils import update_kwargs


class _HSFrontier(_Frontier):
    """Modified hubstorage Frontier with newcount per slot."""

    def __init__(self, *args, **kwargs):
        super(_HSFrontier, self).__init__(*args, **kwargs)
        self.newcount = defaultdict(int)

    def _get_writer(self, frontier, slot):
        """Modified helper method to create a batchuploader writer with updated
        callback to write newcount data per slot.

        :return: a batchuploader writer instance.
        :rtype: :class:`~scrapinghub.hubstorage.batchuploader._BatchWriter`
        """
        key = (frontier, slot)
        writer = self._writers.get(key)
        if not writer:
            writer = self.client.batchuploader.create_writer(
                url=urlpathjoin(self.url, frontier, 's', slot),
                auth=self.auth,
                size=self.batch_size,
                start=self.batch_start,
                interval=self.batch_interval,
                qsize=self.batch_qsize,
                content_encoding=self.batch_content_encoding,
                callback=partial(self._writer_callback, key),
            )
            self._writers[key] = writer
        return writer

    def _writer_callback(self, key, response):
        """Writer callback function when new batch is added."""
        self.newcount[key] += response.json()["newcount"]


class Frontiers(_Proxy):
    """Frontiers collection for a project.

    Not a public constructor: use :class:`~scrapinghub.client.projects.Project`
    instance to get a :class:`Frontiers` instance.
    See :attr:`~scrapinghub.client.Project.frontiers` attribute.

    Usage:

    - get all frontiers from a project::

        >>> project.frontiers.iter()
        <list_iterator at 0x103c93630>

    - list all frontiers::

        >>> project.frontiers.list()
        ['test', 'test1', 'test2']

    - get a frontier by name::

        >>> project.frontiers.get('test')
        <scrapinghub.client.frontiers.Frontier at 0x1048ae4a8>

    - flush data of all frontiers of a project::

        >>> project.frontiers.flush()

    - show amount of new requests added for all frontiers::

        >>> project.frontiers.newcount
        3

    - close batch writers of all frontiers of a project::

        >>> project.frontiers.close()
    """
    def __init__(self, *args, **kwargs):
        super(Frontiers, self).__init__(*args, **kwargs)

    def get(self, name):
        """Get a frontier by name.

        :param name: a frontier name string.
        :return: a frontier instance.
        :rtype: :class:`Frontier`
        """
        return Frontier(self._client, self, name)

    def iter(self):
        """Iterate through frontiers.

        :return: an iterator over frontiers names.
        :rtype: :class:`collections.Iterable[str]`
        """
        return iter(self.list())

    def list(self):
        """List frontiers names.

        :return: a list of frontiers names.
        :rtype: :class:`list[str]`
        """
        return next(self._origin.apiget('list'))

    @property
    def newcount(self):
        """Integer amount of new entries added to all frontiers."""
        return sum(self._origin.newcount.values())

    def flush(self):
        """Flush data in all frontiers writer threads."""
        self._origin.flush()

    def close(self):
        """Close frontier writer threads one-by-one."""
        self._origin.close()


class Frontier(object):
    """Representation of a frontier object.

    Not a public constructor: use :class:`Frontiers` instance to get a
    :class:`Frontier` instance. See :meth:`Frontiers.get` method.

    Usage:

    - get iterator with all slots::

        >>> frontier.iter()
        <list_iterator at 0x1030736d8>

    - list all slots::

        >>> frontier.list()
        ['example.com', 'example.com2']

    - get a slot by name::

        >>> frontier.get('example.com')
        <scrapinghub.client.frontiers.FrontierSlot at 0x1049d8978>

    - flush frontier data::

        >>> frontier.flush()

    - show amount of new requests added to frontier::

        >>> frontier.newcount
        3
    """
    def __init__(self, client, frontiers, name):
        self.key = name
        self._client = client
        self._frontiers = frontiers

    def get(self, slot):
        """Get a slot by name.

        :return: a frontier slot instance.
        :rtype: :class:`FrontierSlot`
        """
        return FrontierSlot(self._client, self, slot)

    def iter(self):
        """Iterate through slots.

        :return: an iterator over frontier slots names.
        :rtype: :class:`collections.Iterable[str]`
        """
        return iter(self.list())

    def list(self):
        """List all slots.

        :return: a list of frontier slots names.
        :rtype: :class:`list[str]`
        """
        return next(self._frontiers._origin.apiget((self.key, 'list')))

    def flush(self):
        """Flush data for a whole frontier."""
        writers = self._frontiers._origin._writers
        for (fname, _), writer in writers.items():
            if fname == self.key:
                writer.flush()

    @property
    def newcount(self):
        """Integer amount of new entries added to frontier."""
        newcount_values = self._frontiers._origin.newcount
        return sum(v for (frontier, _), v in newcount_values.items()
                   if frontier == self.key)


class FrontierSlot(object):
    """Representation of a frontier slot object.

    Not a public constructor: use :class:`Frontier` instance to get a
    :class:`FrontierSlot` instance. See :meth:`Frontier.get` method.

    Usage:

    - add request to a queue::

        >>> data = [{'fp': 'page1.html', 'p': 1, 'qdata': {'depth': 1}}]
        >>> slot.q.add('example.com', data)

    - add fingerprints to a slot::

        >>> slot.f.add(['fp1', 'fp2'])

    - flush data for a slot::

        >>> slot.flush()

    - show amount of new requests added to a slot::

        >>> slot.newcount
        2

    - read requests from a slot::

        >>> slot.q.iter()
        <generator object jldecode at 0x1049aa9e8>
        >>> slot.q.list()
        [{'id': '0115a8579633600006',
          'requests': [['page1.html', {'depth': 1}]]}]

    - read fingerprints from a slot::

        >>> slot.f.iter()
        <generator object jldecode at 0x103de4938>
        >>> slot.f.list()
        ['page1.html']

    - delete a batch with requests from a slot::

        >>> slot.q.delete('0115a8579633600006')

    - delete a whole slot::

        >>> slot.delete()
    """
    def __init__(self, client, frontier, slot):
        self.key = slot
        self._client = client
        self._frontier = frontier
        self.fingerprints = FrontierSlotFingerprints(self)
        self.queue = FrontierSlotQueue(self)

    @property
    def f(self):
        """Shortcut to have quick access to slot fingerprints.

        :return: fingerprints collection for the slot.
        :rtype: :class:`FrontierSlotFingerprints`
        """
        return self.fingerprints

    @property
    def q(self):
        """Shortcut to have quick access to a slot queue.

        :return: queue instance for the slot.
        :rtype: :class:`FrontierSlotQueue`
        """
        return self.queue

    def delete(self):
        """Delete the slot."""
        origin = self._frontier._frontiers._origin
        origin.delete_slot(self._frontier.key, self.key)
        origin.newcount.pop((self._frontier.key, self.key), None)

    def flush(self):
        """Flush data for the slot."""
        writers = self._frontier._frontiers._origin._writers
        writer = writers.get((self._frontier.key, self.key))
        if writer:
            writer.flush()

    @property
    def newcount(self):
        """Integer amount of new entries added to slot."""
        newcount_values = self._frontier._frontiers._origin.newcount
        return newcount_values.get((self._frontier.key, self.key), 0)


class FrontierSlotFingerprints(object):
    """Representation of request fingerprints collection stored in slot."""

    def __init__(self, slot):
        self.key = slot.key
        self._frontier = slot._frontier
        self._slot = slot

    def add(self, fps):
        """Add new fingerprints to slot.

        :param fps: a list of string fingerprints to add.
        """
        origin = self._frontier._frontiers._origin
        writer = origin._get_writer(self._frontier.key, self.key)
        fps = list(fps) if not isinstance(fps, list) else fps
        if not all(isinstance(fp, string_types) for fp in fps):
            raise ValueError('Fingerprint should be of a string type')
        for fp in fps:
            writer.write({'fp': fp})

    def iter(self, **params):
        """Iterate through fingerprints in the slot.

        :param \*\*params: (optional) additional query params for the request.
        :return: an iterator over fingerprints.
        :rtype: :class:`collections.Iterable[str]`
        """
        origin = self._frontier._frontiers._origin
        path = (self._frontier.key, 's', self.key, 'f')
        for fp in origin.apiget(path, params=params):
            yield fp.get('fp')

    def list(self, **params):
        """List fingerprints in the slot.

        :param \*\*params: (optional) additional query params for the request.
        :return: a list of fingerprints.
        :rtype: :class:`list[str]`
        """
        return list(self.iter(**params))


class FrontierSlotQueue(object):
    """Representation of request batches queue stored in slot."""

    def __init__(self, slot):
        self.key = slot.key
        self._frontier = slot._frontier
        self._slot = slot

    def add(self, fps):
        """Add requests to the queue."""
        origin = self._frontier._frontiers._origin
        return origin.add(self._frontier.key, self.key, fps)

    def iter(self, mincount=None, **params):
        """Iterate through batches in the queue.

        :param mincount: (optional) limit results with min amount of requests.
        :param \*\*params: (optional) additional query params for the request.
        :return: an iterator over request batches in the queue where each
            batch is represented with a dict with ('id', 'requests') field.
        :rtype: :class:`collections.Iterable[dict]`
        """
        origin = self._frontier._frontiers._origin
        path = (self._frontier.key, 's', self.key, 'q')
        update_kwargs(params, mincount=mincount)
        return origin.apiget(path, params=params)

    def list(self, mincount=None, **params):
        """List request batches in the queue.

        :param mincount: (optional) limit results with min amount of requests.
        :param \*\*params: (optional) additional query params for the request.
        :return: a list of request batches in the queue where each batch
            is represented with a dict with ('id', 'requests') field.
        :rtype: :class:`list[dict]`
        """
        return list(self.iter(mincount=mincount, **params))

    def delete(self, ids):
        """Delete request batches from the queue."""
        origin = self._frontier._frontiers._origin
        return origin.delete(self._frontier.key, self.key, ids)
