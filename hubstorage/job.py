import logging
from collections import MutableMapping
from .resourcetype import ResourceType, ItemsResourceType
from .utils import millitime, urlpathjoin


class Job(object):

    def __init__(self, client, key, auth=None, metadata=None):
        self.key = urlpathjoin(key)
        assert len(self.key.split('/')) == 3, 'Jobkey must be projectid/spiderid/jobid: %s' % self.key
        self.items = Items(client, self.key, auth)
        self.logs = Logs(client, self.key, auth)
        self.samples = Samples(client, self.key, auth)
        self.metadata = JobMeta(client, self.key, auth)

    def jobauth(self):
        return self.key, self.metadata.authtoken()

    def update(self, *args, **kwargs):
        kwargs.setdefault('updated_time', millitime())
        self.metadata.update(*args, **kwargs)
        self.metadata.save()

    def started(self):
        self.update(state='running', started_time=millitime())

    def finished(self, close_reason=None):
        data = {'state': 'finished'}
        if 'close_reason' not in self.metadata:
            data['close_reason'] = close_reason or 'no_reason'
        self.update(data)

    def failed(self, reason, message=None):
        if message:
            self.logs.error(message, appendmode=True)
        self.finished(reason)

    def purged(self):
        self.update(state='purged')

    def stop(self):
        self.update(stop_requested=True)


class JobMeta(ResourceType, MutableMapping):

    resource_type = 'jobs'
    _cached = None

    def __init__(self, *a, **kw):
        super(JobMeta, self).__init__(*a, **kw)
        self._cached = None
        self._deleted = set()

    @property
    def _data(self):
        if self._cached is None:
            r = self.apiget()
            try:
                self._cached = r.next()
            except StopIteration:
                self._cached = {}

        return self._cached

    def expire(self):
        self._cached = None

    def save(self):
        for key in self._deleted:
            self.apidelete(key)
        self._deleted.clear()
        if self._cached:
            data = dict((k, v) for k, v in self._data.iteritems()
                        if k not in ('auth', '_key'))
            self.apipost(jl=data)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        self._deleted.discard(key)

    def __delitem__(self, key):
        del self._data[key]
        self._deleted.add(key)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def summary(self):
        return self.apiget('summary').next()

    def authtoken(self):
        return self.apiget('auth').next()


class Logs(ItemsResourceType):

    resource_type = 'logs'
    batch_content_encoding = 'gzip'

    def log(self, message, level=logging.INFO, ts=None, appendmode=False, **other):
        other.update(message=message, level=level, time=ts or millitime())
        if self._writer is None:
            self.batch_append = appendmode
        self.write(other)

    def debug(self, message, **other):
        self.log(message, level=logging.DEBUG, **other)

    def info(self, message, **other):
        self.log(message, level=logging.INFO, **other)

    def warn(self, message, **other):
        self.log(message, level=logging.WARNING, **other)
    warning = warn

    def error(self, message, **other):
        self.log(message, level=logging.ERROR, **other)


class Items(ItemsResourceType):

    resource_type = 'items'
    batch_content_encoding = 'gzip'


class Samples(ItemsResourceType):

    resource_type = 'samples'

    def stats(self):
        raise NotImplementedError('Resource does not expose stats')
