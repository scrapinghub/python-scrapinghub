import logging
from .resourcetype import ResourceType
from .utils import millitime, urlpathjoin


class Job(object):

    def __init__(self, client, key, auth=None, metadata=None):
        self.key = urlpathjoin(key)
        assert len(self.key.split('/')) == 3, 'Jobkey must be projectid/spiderid/jobid: %s' % self.key
        self._metadata = metadata
        self.jobsmeta = JobsMeta(client, self.key, auth)
        self.items = Items(client, self.key, auth)
        self.logs = Logs(client, self.key, auth)
        self.samples = Samples(client, self.key, auth)

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self.jobsmeta.get().next()
        return self._metadata

    @property
    def stats(self):
        if self._stats is None:
            self._stats = self.jobsmeta.get_stats()
        return self._stats

    def expire(self):
        self._stats = None
        self._metadata = None

    def update(self, *args, **kwargs):
        kwargs.setdefault('updated_time', millitime())
        self.jobsmeta.update(*args, **kwargs)
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


class JobsMeta(ResourceType):

    resource_type = 'jobs'

    def get(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def set(self, key, value):
        self.update(key=value)

    def delete(self, _key):
        self.apidelete(_key)

    def update(self, *args, **kwargs):
        self.apipost(jl=dict(*args, **kwargs))

    def get_stats(self):
        return self.apiget('stats').next()


class Logs(ResourceType):

    resource_type = 'logs'

    _offset = None
    def _increase_offset(self, step=1):
        if self._offset is None:
            stats = self.apiget('stats').next()
            self._offset = stats.get('totals', {}).get('input_values', -1)
        self._offset += step
        return self._offset

    def get(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def log(self, message, level=logging.INFO, ts=None, **other):
        if ts is None:
            ts = millitime()

        other.update(message=message, level=level, time=ts)
        return self.apipost(jl=other, params={'start': self._increase_offset()}) 

    def debug(self, message, **other):
        self.log(message, level=logging.DEBUG, **other)

    def info(self, message, **other):
        self.log(message, level=logging.INFO, **other)

    def warn(self, message, **other):
        self.log(message, level=logging.WARNING, **other)
    warning = warn

    def error(self, message, **other):
        self.log(message, level=logging.ERROR, **other)


class Samples(ResourceType):

    resource_type = 'samples'


class Items(ResourceType):

    resource_type = 'items'

    def get(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def write(self, data):
        self.apipost(jl=data)

