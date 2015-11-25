import logging
from .resourcetype import (ItemsResourceType, DownloadableResource,
    MappingResourceType)
from .utils import millitime, urlpathjoin
from .jobq import JobQ


class Job(object):

    def __init__(self, client, key, auth=None, jobauth=None, metadata=None):
        self.key = urlpathjoin(key)
        assert len(self.key.split('/')) == 3, \
            'Jobkey must be projectid/spiderid/jobid: %s' % self.key
        self.jobauth = jobauth
        self.auth = self.jobauth or auth
        self.metadata = JobMeta(client, self.key, self.auth, cached=metadata)
        self.items = Items(client, self.key, self.auth)
        self.logs = Logs(client, self.key, self.auth)
        self.samples = Samples(client, self.key, self.auth)
        self.requests = Requests(client, self.key, self.auth)
        self.jobq = JobQ(client, self.key.split('/')[0], auth)

    def close_writers(self):
        wl = [self.items, self.logs, self.samples, self.requests]
        # close all resources that use background writers
        for w in wl:
            w.close(block=False)
        # now wait for all writers to close together
        for w in wl:
            w.close(block=True)

    def update_metadata(self, *args, **kwargs):
        self.metadata.update(*args, **kwargs)
        self.metadata.save()
        self.metadata.expire()

    def request_cancel(self):
        self.jobq.request_cancel(self)

    def purged(self):
        self.jobq.delete(self)
        self.metadata.expire()


class JobMeta(MappingResourceType):

    resource_type = 'jobs'
    ignore_fields = set(('auth', '_key', 'state'))

    def authtoken(self):
        return self.liveget('auth')


class Logs(ItemsResourceType, DownloadableResource):

    resource_type = 'logs'
    batch_content_encoding = 'gzip'

    def __init__(self, client, key, auth=None, appendmode=False):
        ItemsResourceType.__init__(self, client, key, auth)
        self.batch_append = appendmode

    def batch_write_start(self):
        if self.batch_append:
            return self.stats().get('totals', {}).get('input_values', 0)
        return 0

    def log(self, message, level=logging.INFO, ts=None, **other):
        other.update(message=message, level=level, time=ts or millitime())
        # legacy support for an appendmode argument. This should be set at
        # object initialization time.
        if self._writer is None and other.get('appendmode'):
            self.batch_append = True
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


class Items(ItemsResourceType, DownloadableResource):

    resource_type = 'items'
    batch_content_encoding = 'gzip'


class Samples(ItemsResourceType):

    resource_type = 'samples'

    def stats(self):
        raise NotImplementedError('Resource does not expose stats')


class Requests(ItemsResourceType, DownloadableResource):

    resource_type = 'requests'
    batch_content_encoding = 'gzip'

    def add(self, url, status, method, rs, parent, duration, ts, fp=None):
        return self.write({
            'url': url,
            'status': int(status),
            'method': method,
            'rs': int(rs),
            'duration': int(duration),
            'parent': parent,
            'time': int(ts),
            'fp': fp,
        })
