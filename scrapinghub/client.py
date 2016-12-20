import json
import logging

from scrapinghub import APIError
from scrapinghub import Connection
from scrapinghub import HubstorageClient

from scrapinghub.hubstorage.activity import Activity
from scrapinghub.hubstorage.frontier import Frontier
from scrapinghub.hubstorage.job import JobMeta
from scrapinghub.hubstorage.project import Reports
from scrapinghub.hubstorage.project import Settings
from scrapinghub.hubstorage.resourcetype import DownloadableResource
from scrapinghub.hubstorage.resourcetype import ItemsResourceType

from scrapinghub.hubstorage.collectionsrt import Collections as _Collections
from scrapinghub.hubstorage.collectionsrt import Collection as _Collection
from scrapinghub.hubstorage.job import Items as _Items
from scrapinghub.hubstorage.job import Logs as _Logs
from scrapinghub.hubstorage.job import Samples as _Samples
from scrapinghub.hubstorage.job import Requests as _Requests



class DuplicateJobError(APIError):
    pass


class LogLevel(object):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    SILENT = CRITICAL + 1


class ScrapinghubClient(object):

    def __init__(self, auth=None, dash_endpoint=None, **kwargs):
        self.connection = Connection(apikey=auth, url=dash_endpoint)
        self.hsclient = HubstorageClient(auth=auth, **kwargs)
        self.projects = Projects(self)

    def get_project(self, projectid):
        return self.projects.get(int(projectid))

    def get_job(self, jobkey):
        # FIXME solve the splitting more gracefully
        projectid = int(jobkey.split('/')[0])
        return self.projects.get(projectid).jobs.get(jobkey)

    def close(self, timeout=None):
        self.hsclient.close(timeout=timeout)


class Projects(object):

    def __init__(self, client):
        self.client = client

    def get(self, projectid):
        return Project(self.client, int(projectid))

    def list(self):
        return self.client.connection.project_ids()

    def summary(self, **params):
        return self.client.hsclient.projects.jobsummaries(**params)


class Project(object):

    def __init__(self, client, projectid):
        self.client = client
        self.id = projectid

        # sub-resources
        self.spiders = Spiders(client, projectid)
        self.jobs = Jobs(client, projectid)

        # proxied sub-resources
        self.activity = Activity(client.hsclient, projectid)
        self.frontier = Frontier(client.hsclient, projectid)
        self.settings = Settings(client.hsclient, projectid)
        self.reports = Reports(client.hsclient, projectid)
        self.collections = Collections(_Collections, client, projectid)


class Spiders(object):

    def __init__(self, client, projectid):
        self.client = client
        self.projectid = projectid

    def get(self, spidername, **params):
        project = self.client.hsclient.get_project(self.projectid)
        spiderid = project.ids.spider(spidername, **params)
        return Spider(self.client, self.projectid, spiderid, spidername)

    def list(self):
        project = self.client.connection[self.projectid]
        return project.spiders()


class Spider(object):

    def __init__(self, client, projectid, spiderid, spidername):
        self.client = client
        self.projectid = projectid
        self.id = spiderid
        self.name = spidername
        self.jobs = Jobs(client, projectid, self)

    def update_tags(self, add=None, remove=None):
        params = _get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'project': self.projectid, 'spider': self.name})
        result = self.client.connection._post('jobs_update', 'json', params)
        return result['count']


class Jobs(object):

    def __init__(self, client, projectid, spider=None):
        self.client = client
        self.projectid = projectid
        self.spider = spider

    @property
    def _hsproject(self):
        """Shortcut to hsclient.project for internal uses"""
        return self.client.hsclient.get_project(self.projectid)

    def count(self, **params):
        if self.spider:
            params['spider'] = self.spider.name
        return next(self._hsproject.jobq.apiget(('count',), params=params))

    def iter(self, **params):
        """ Iterate over jobs collection for a given set of params.
        FIXME the function returns a list of dicts, not a list of Job's.
        """
        if self.spider:
            params['spider'] = self.spider.name
        return self._hsproject.jobq.list(**params)

    def schedule(self, spidername=None, **params):
        if not spidername and not self.spider:
            raise APIError('Please provide spidername')
        params['project'] = self.projectid
        params['spider'] = spidername or self.spider.name
        if 'meta' in params:
            params['meta'] = json.dumps(params['meta'])
        # FIXME JobQ endpoint can schedule multiple jobs with json-lines,
        # corresponding Dash endpoint - only one job per request
        response = self.client.connection._post('schedule', 'json', params)
        if response.get('status') == 'error':
            if 'already scheduled' in response['message']:
                raise DuplicateJobError(response['message'])
            raise APIError(response['message'])
        return Job(self.client, response['jobid'])

    def get(self, jobkey):
        projectid, spiderid, jobid = jobkey.split('/')
        if int(projectid) != self.projectid:
            raise APIError('Please use same project id')
        if self.spider and int(spiderid) != self.spider.id:
            raise APIError('Please use same spider id')
        return Job(self.client, jobkey)

    def summary(self, _queuename=None, **params):
        spiderid = None if not self.spider else self.spider.id
        return self._hsproject.jobq.summary(
            _queuename, spiderid=spiderid, **params)

    def lastjobsummary(self, **params):
        spiderid = None if not self.spider else self.spider.id
        summ = self._hsproject.spiders.lastjobsummary(spiderid, **params)
        # FIXME original lastjobsummary returns a generator
        return list(summ)


class Job(object):

    def __init__(self, client, jobkey, metadata=None):
        self.client = client
        self.key = jobkey
        self.projectid = int(jobkey.split('/')[0])

        # proxied sub-resources
        self.items = Items(_Items, client, jobkey)
        self.logs = Logs(_Logs, client, jobkey)
        self.requests = Requests(_Requests, client, jobkey)
        self.samples = Samples(_Samples, client, jobkey)

        self.metadata = JobMeta(client.hsclient, jobkey, cached=metadata)

    def update_metadata(self, *args, **kwargs):
        self.client.hsclient.get_job(self.key).update_metadata(*args, **kwargs)

    def update_tags(self, add=None, remove=None):
        params = _get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'project': self.projectid, 'job': self.key})
        result = self.client.connection._post('jobs_update', 'json', params)
        return result['count']

    def close_writers(self):
        self.client.hsclient.get_job(self.key).close_writers()

    @property
    def _hsproject(self):
        """Shortcut to hsclient.project for internal uses"""
        return self.client.hsclient.get_project(self.projectid)

    def start(self, **params):
        return self._hsproject.jobq.start(self, **params)

    def update(self, **params):
        return self._hsproject.jobq.update(self, **params)

    def cancel(self):
        self._hsproject.jobq.request_cancel(self)

    def finish(self, **params):
        return self._hsproject.jobq.finish(self, **params)

    def delete(self, **params):
        return self._hsproject.jobq.delete(self, **params)

    def purge(self):
        self.client.hsclient.get_job(self.key).purged()
        self.metadata.expire()


class _Proxy(object):
    """A proxy to create a class instance and proxy its methods to entity."""

    def __init__(self, cls, client, key):
        self.key = key
        self.client = client
        self._origin = cls(client.hsclient, key)
        if issubclass(cls, ItemsResourceType):
            self._proxy_methods(['get', 'write', 'flush', 'close',
                                 'stats', ('iter', 'list')])
        # DType iter_values() has more priority than IType list()
        if issubclass(cls, DownloadableResource):
            self._proxy_methods([('iter', 'iter_values'),
                                 ('iter_raw_msgpack', 'iter_msgpack'),
                                 ('iter_raw_json', 'iter_json')])

    def _proxy_methods(self, methods):
        _proxy_methods(self._origin, self, methods)


class Logs(_Proxy):

    def __init__(self, *args, **kwargs):
        super(Logs, self).__init__(*args, **kwargs)
        self._proxy_methods(['log', 'debug', 'info', 'warning', 'warn',
                             'error', 'batch_write_start'])

    def iter(self, **params):
        params = self._apply_iter_filters(params)
        return self._origin.iter_values(**params)

    def iter_raw_json(self, **params):
        params = self._apply_iter_filters(params)
        return self._origin.iter_json(**params)

    def iter_raw_msgpack(self, **params):
        params = self._apply_iter_filters(params)
        return self._origin.iter_msgpack(**params)

    def _apply_iter_filters(self, params):
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '%s/%s' % (self.key, offset)
        level = params.pop('level', None)
        if level:
            minlevel = getattr(LogLevel, level, None)
            if minlevel is None:
                raise APIError("Unknown log level: %s" % level)
            params['filter'] = json.dumps(['level', '>=', [minlevel]])
        return params


class Items(_Proxy):

    def iter(self, **params):
        params = self._apply_iter_filters(params)
        return self._origin.iter_values(**params)

    def iter_raw_json(self, **params):
        params = self._apply_filters(params)
        return self._origin.iter_json(**params)

    def iter_raw_msgpack(self, **params):
        params = self._apply_filters(params)
        return self._origin.iter_msgpack(**params)

    def _apply_iter_filters(self, params):
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '%s/%s' % (self.key, params['offset'])
        return params


class Requests(_Proxy):

    def __init__(self, *args, **kwargs):
        super(Requests, self).__init__(*args, **kwargs)
        self._proxy_methods(['add'])


class Samples(_Proxy):
    pass


class Collections(_Proxy):

    def __init__(self, *args, **kwargs):
        super(Collections, self).__init__(*args, **kwargs)
        self._proxy_methods([
            'count', 'get', 'set', 'delete', 'create_writer',
            '_validate_collection',
        ])

    def new_store(self, colname):
        return self.new_collection('s', colname)

    def new_cached_store(self, colname):
        return self.new_collection('cs', colname)

    def new_versioned_store(self, colname):
        return self.new_collection('vs', colname)

    def new_versioned_cached_store(self, colname):
        return self.new_collection('vcs', colname)

    def new_collection(self, coltype, colname):
        self._validate_collection(coltype, colname)
        return Collection(self.client, self, coltype, colname)


class Collection(object):

    def __init__(self, client, collections, coltype, colname):
        self.client = client
        # FIXME it'd be nice to reuse _Proxy here, but Collection init is
        # a bit custom: there's a compound key and required collections
        # field to create an origin instance
        self._origin = _Collection(coltype, colname, collections._origin)
        _proxy_methods(self._origin, self, [
            'create_writer', 'get', 'set', 'delete', 'count',
            ('iter', 'iter_values'), ('iter_raw_json', 'iter_json'),
        ])


def _get_tags_for_update(**kwargs):
    """Helper to check tags changes"""
    params = {}
    for k, v in kwargs.items():
        if not v:
            continue
        if not isinstance(v, list):
            raise APIError("Add/remove field value must be a list")
        params[k] = v
    return params


def _proxy_methods(origin, successor, methods):
    """A helper to proxy methods from origin to successor.

    Accepts a list with strings and tuples:
    - each string defines:
        a successor method name to proxy 1:1 with origin method
    - each tuple should consist of 2 strings:
        a successor method name and an origin method name
    """
    for method in methods:
        if isinstance(method, tuple):
            successor_name, origin_name = method
        else:
            successor_name, origin_name = method, method
        if not hasattr(successor, successor_name):
            setattr(successor, successor_name, getattr(origin, origin_name))
