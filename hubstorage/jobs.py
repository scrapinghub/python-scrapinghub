import logging
from .resourcetype import ResourceType
from .utils import millitime


class Jobs(ResourceType):

    resource_type = 'jobs'

    def __init__(self, key, client, auth):
        super(Jobs, self).__init__(key, client, auth)
        self.items = Items(key, client, auth)
        self.logs = Logs(key, client, auth)
        self.samples = Samples(key, client, auth)
        self._metadata = None

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self.apiget().next()
        return self._metadata

    @property
    def stats(self):
        if self._metadata is None:
            self._metadata = self.apiget('stats').next()
        return self._metadata

    def expire(self):
        self._metadata = None
        self._stats = None

    def update(self, *args, **kwargs):
        data = dict(*args, **kwargs)
        data.setdefault('updated_time', millitime())
        self.apipost(jl=data)
        self.metadata.update(data)

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


class Logs(ResourceType):

    resource_type = 'logs'

    _offset = None
    def _increase_offset(self, step=1):
        if self._offset is None:
            stats = self.apiget('stats').next()
            self._offset = stats.get('totals', {}).get('input_values', -1)
        self._offset += step
        return self._offset

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

