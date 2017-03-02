from __future__ import absolute_import
from functools import partial
from collections import defaultdict

from six import string_types

from scrapinghub.hubstorage.frontier import Frontier as _Frontier
from scrapinghub.hubstorage.utils import urlpathjoin

from .utils import _Proxy


class _HSFrontier(_Frontier):
    """Modified hubstorage Frontier with newcount per slot."""

    def __init__(self, *args, **kwargs):
        super(_HSFrontier, self).__init__(*args, **kwargs)
        self.newcount = defaultdict(int)

    def _get_writer(self, frontier, slot):
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
        self.newcount[key] += response.json()["newcount"]


class Frontiers(_Proxy):
    """Frontiers collection for a project.

    Not a public constructor: use :class:`Project` instance to get a
    :class:`Frontiers` instance. See :attr:`Project.frontiers` attribute.

    Usage:

    - get all frontiers from a project::
        >>> project.frontiers.iter()
        <list_iterator at 0x103c93630>

    - list all frontiers
        >>> project.frontiers.list()
        ['test', 'test1', 'test2']

    - get a frontier by name
        >>> project.frontiers.get('test')
        <scrapinghub.client.Frontier at 0x1048ae4a8>

    - flush data of all frontiers of a project
        >>> project.frontiers.flush()

    - show amount of new requests added for all frontiers
        >>> project.frontiers.newcount
        3

    - close batch writers of all frontiers of a project
        >>> project.frontiers.close()
    """
    def __init__(self, *args, **kwargs):
        super(Frontiers, self).__init__(*args, **kwargs)
        self._proxy_methods(['close', 'flush'])

    def get(self, name):
        """Get a frontier by name."""
        return Frontier(self._client, self, name)

    def iter(self):
        """Iterate through frontiers."""
        return iter(self.list())

    def list(self):
        """List frontiers."""
        return next(self._origin.apiget('list'))

    @property
    def newcount(self):
        return sum(self._origin.newcount.values())


class Frontier(object):
    """Representation of a frontier object.

    Not a public constructor: use :class:`Frontiers` instance to get a
    :class:`Frontier` instance. See :meth:`Frontiers.get` method.

    Usage:

    - get iterator with all slots
        >>> frontier.iter()
        <list_iterator at 0x1030736d8>

    - list all slots
        >>> frontier.list()
        ['example.com', 'example.com2']

    - get a slot by name
        >>> frontier.get('example.com')
        <scrapinghub.client.FrontierSlot at 0x1049d8978>

    - flush frontier data
        >>> frontier.flush()

    - show amount of new requests added to frontier
        >>> frontier.newcount
        3
    """
    def __init__(self, client, frontiers, name):
        self.key = name
        self._client = client
        self._frontiers = frontiers

    def get(self, slot):
        """Get a slot by name."""
        return FrontierSlot(self._client, self, slot)

    def iter(self):
        """Iterate through slots."""
        return iter(self.list())

    def list(self):
        """List all slots."""
        return next(self._frontiers._origin.apiget((self.key, 'list')))

    def flush(self):
        """Flush data for a whole frontier."""
        writers = self._frontiers._origin._writers
        for (fname, _), writer in writers.items():
            if fname == self.key:
                writer.flush()

    @property
    def newcount(self):
        newcount_values = self._frontiers._origin.newcount
        return sum(v for (frontier, _), v in newcount_values.items()
                   if frontier == self.key)


class FrontierSlot(object):
    """Representation of a frontier slot object.

    Not a public constructor: use :class:`Frontier` instance to get a
    :class:`FrontierSlot` instance. See :meth:`Frontier.get` method.

    Usage:

    - add request to a queue
        >>> data = [{'fp': 'page1.html', 'p': 1, 'qdata': {'depth': 1}}]
        >>> slot.q.add('example.com', data)

    - add fingerprints to a slot
        >>> slot.f.add(['fp1', 'fp2'])

    - flush data for a slot
        >>> slot.flush()

    - show amount of new requests added to a slot
        >>> slot.newcount
        2

    - read requests from a slot
        >>> slot.q.iter()
        <generator object jldecode at 0x1049aa9e8>
        >>> slot.q.list()
        [{'id': '0115a8579633600006',
          'requests': [['page1.html', {'depth': 1}]]}]

    - read fingerprints from a slot
        >>> slot.f.iter()
        <generator object jldecode at 0x103de4938>
        >>> slot.f.list()
        ['page1.html']

    - delete a batch with requests from a slot
        >>> slot.q.delete('0115a8579633600006')

    - delete a whole slot
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
        return self.fingerprints

    @property
    def q(self):
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
        newcount_values = self._frontier._frontiers._origin.newcount
        return newcount_values.get((self._frontier.key, self.key), 0)


class FrontierSlotFingerprints(object):

    def __init__(self, slot):
        self.key = slot.key
        self._frontier = slot._frontier
        self._slot = slot

    def add(self, fps):
        origin = self._frontier._frontiers._origin
        writer = origin._get_writer(self._frontier.key, self.key)
        fps = list(fps) if not isinstance(fps, list) else fps
        if not all(isinstance(fp, string_types) for fp in fps):
            raise ValueError('Fingerprint should be of a string type')
        for fp in fps:
            writer.write({'fp': fp})

    def iter(self, **kwargs):
        """Iterate through fingerprints in the slot."""
        origin = self._frontier._frontiers._origin
        path = (self._frontier.key, 's', self.key, 'f')
        for fp in origin.apiget(path, params=kwargs):
            yield fp.get('fp')

    def list(self, **kwargs):
        """List fingerprints in the slot."""
        return list(self.iter(**kwargs))


class FrontierSlotQueue(object):

    def __init__(self, slot):
        self.key = slot.key
        self._frontier = slot._frontier
        self._slot = slot

    def add(self, fps):
        """Add requests to the queue."""
        origin = self._frontier._frontiers._origin
        return origin.add(self._frontier.key, self.key, fps)

    def iter(self, **kwargs):
        """Iterate through batches in the queue."""
        origin = self._frontier._frontiers._origin
        path = (self._frontier.key, 's', self.key, 'q')
        return origin.apiget(path, params=kwargs)

    def list(self, **kwargs):
        """List request batches in the queue."""
        return list(self.iter(**kwargs))

    def delete(self, ids):
        """Delete request batches from the queue."""
        origin = self._frontier._frontiers._origin
        return origin.delete(self._frontier.key, self.key, ids)
