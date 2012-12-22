import logging
from .resourcetype import ResourceType, ItemsResourceType
from .utils import millitime, urlpathjoin


class Job(object):

    def __init__(self, client, key, auth=None, metadata=None):
        self.key = urlpathjoin(key)
        assert len(self.key.split('/')) == 3, 'Jobkey must be projectid/spiderid/jobid: %s' % self.key
        self._metadata = metadata
        self.jobs = Jobs(client, self.key, auth)
        self.items = Items(client, self.key, auth)
        self.logs = Logs(client, self.key, auth)
        self.samples = Samples(client, self.key, auth)

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self.jobs.get().next()
        return self._metadata

    def expire(self):
        self._metadata = None

    def update(self, *args, **kwargs):
        kwargs.setdefault('updated_time', millitime())
        self.jobs.update(*args, **kwargs)
        self.metadata.update(*args, **kwargs)

    def started(self):
        self.update(state='running', started_time=millitime())

    def finished(self, close_reason=None):
        data = {'state': 'finished'}
        if 'close_reason' not in self.metadata:
            data['close_reason'] = close_reason or 'no_reason'
        self.update(data)

    def failed(self, reason, message=None):
        if message:
            self.logs.error(message)
        self.finished(reason)

    def purged(self):
        self.update(state='purged')

    def stop(self):
        self.update(stop_requested=True)


class Jobs(ResourceType):

    resource_type = 'jobs'

    def get(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def set(self, key, value):
        self.update(key=value)

    def delete(self, _key):
        self.apidelete(_key)

    def update(self, *args, **kwargs):
        self.apipost(jl=dict(*args, **kwargs))


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


class Samples(ItemsResourceType):

    resource_type = 'samples'


class Items(ItemsResourceType):

    resource_type = 'items'
    batch_content_encoding = 'gzip'
