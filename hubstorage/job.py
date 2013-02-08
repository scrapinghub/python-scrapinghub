import logging
from collections import MutableMapping
from .resourcetype import ResourceType, ItemsResourceType
from .utils import millitime, urlpathjoin
from .jobq import JobQ


class Job(object):

    def __init__(self, client, key, auth=None, jobauth=None, metadata=None):
        self.key = urlpathjoin(key)
        assert len(self.key.split('/')) == 3, 'Jobkey must be projectid/spiderid/jobid: %s' % self.key
        self._jobauth = jobauth
        # It can't use self.jobauth because metadata is not ready yet
        self.auth = jobauth or auth
        self.metadata = JobMeta(client, self.key, self.auth, metadata=metadata)
        self.items = Items(client, self.key, self.auth)
        self.logs = Logs(client, self.key, self.auth)
        self.samples = Samples(client, self.key, self.auth)
        self.jobq = JobQ(client, self.key.split('/')[0], auth)

    def close_writers(self):
        wl = [self.items, self.logs, self.samples]
        # close all resources that use backwround writers
        for w in wl:
            w.close(block=False)
        # now wait for all writers to close together
        for w in wl:
            w.close(block=True)

    @property
    def jobauth(self):
        if self._jobauth is None:
            token = self.metadata.authtoken()
            self._jobauth = (self.key, token)
        return self._jobauth

    def _update_metadata(self, *args, **kwargs):
        self.metadata.update(*args, **kwargs)
        self.metadata.save()
        self.metadata.expire()

    def finished(self, close_reason=None):
        self.close_writers()
        self.metadata.expire()
        close_reason = close_reason or \
            self.metadata.liveget('close_reason') or 'no_reason'
        self._update_metadata(close_reason=close_reason)
        self.jobq.finish(self)

    def failed(self, reason='failed', message=None):
        if message:
            self.logs.error(message, appendmode=True)
        self.finished(reason)

    def purged(self):
        self.jobq.delete(self)
        self.metadata.expire()

    def stop(self):
        self._update_metadata(stop_requested=True)


class JobMeta(ResourceType, MutableMapping):

    resource_type = 'jobs'
    _cached = None

    def __init__(self, *a, **kw):
        self._cached = kw.pop('metadata', None)
        self._deleted = set()
        super(JobMeta, self).__init__(*a, **kw)

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
                        if k not in ('auth', '_key', 'state'))
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

    def authtoken(self):
        return self.liveget('auth')

    def liveget(self, key):
        for o in self.apiget(key):
            return o


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
