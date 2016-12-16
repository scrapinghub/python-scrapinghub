import json
import logging

from scrapinghub import Connection
from scrapinghub import HubstorageClient

from scrapinghub.hubstorage.activity import Activity
from scrapinghub.hubstorage.frontier import Frontier
from scrapinghub.hubstorage.project import Reports
from scrapinghub.hubstorage.project import Settings
from scrapinghub.hubstorage.job import JobMeta

from scrapinghub.hubstorage.job import Items as _Items
from scrapinghub.hubstorage.job import Logs as _Logs
from scrapinghub.hubstorage.job import Samples as _Samples
from scrapinghub.hubstorage.job import Requests as _Requests
from scrapinghub.hubstorage.collectionsrt import Collections as _Collections


class ScrapinghubAPIError(Exception):
    pass


class DuplicateJobError(ScrapinghubAPIError):
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
        self.collections = Collections(client, projectid)
        self.spiders = Spiders(client, projectid)
        self.jobs = Jobs(client, projectid)

        # proxied sub-resources
        self.activity = Activity(client.hsclient, projectid)
        self.frontier = Frontier(client.hsclient, projectid)
        self.settings = Settings(client.hsclient, projectid)
        self.reports = Reports(client.hsclient, projectid)


class Spiders(object):

    def __init__(self, client, projectid):
        self.client = client
        self.projectid = projectid

    def get(self, spidername):
        project = self.client.hsclient.get_project(self.projectid)
        spiderid = project.ids.spider(spidername)
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
            raise ScrapinghubAPIError('Please provide spidername')
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
            raise ScrapinghubAPIError(response['message'])
        return Job(self.client, response['jobid'])

    def get(self, jobkey):
        projectid, spiderid, jobid = jobkey.split('/')
        if int(projectid) != self.projectid:
            raise ScrapinghubAPIError('Please use same project id')
        if self.spider and int(spiderid) != self.spider.id:
            raise ScrapinghubAPIError('Please use same spider id')
        return Job(self.client, jobkey)

    def summary(self, **params):
        spiderid = None if not self.spider else self.spider.id
        return self._hsproject.jobq.summary(spiderid=spiderid, **params)

    def lastjobsummary(self, **params):
        spiderid = None if not self.spider else self.spider.id
        summ = self._hsproject.spiders.lastjobsummary(spiderid, **params)
        # FIXME original lastjobsummary returns a generator
        return list(summ)


class Job(object):

    def __init__(self, client, jobkey, metadata=None):
        self.client = client
        self.key = jobkey
        self.projectid = jobkey.split('/')[0]

        # proxied sub-resources
        self.items = Items(client, jobkey)
        self.logs = Logs(client, jobkey)
        self.requests = Requests(client, jobkey)
        self.samples = Samples(client, jobkey)

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


class EntityProxy(object):
    """A proxy to create a class instance and proxy its methods to entity."""

    def __init__(self, cls, client, key, download_api=False, items_api=False):
        self.client = client
        self.key = key
        self._entity = cls(client.hsclient, key)
        if items_api:
            self._proxy_methods(['get', 'write', 'flush', 'close',
                                 'stats', ('iter', 'list')])
        # DType iter_values() has more priority than IType list()
        if download_api:
            self._proxy_methods([('iter', 'iter_values'),
                                 ('iter_raw_msgpack', 'iter_msgpack'),
                                 ('iter_raw_json', 'iter_json')])

    def _proxy_methods(self, methods):
        """A helper to proxy methods to self._entity object.

        Accepts a list with strings and tuples:
        - each string defines a method name to proxy 1:1 with entity
        - each tuple should consist of 2 strings:
          object method name and original method name in entity
        """
        for method in methods:
            if isinstance(method, tuple):
                name, entity_name = method
            else:
                name, entity_name = method, method
            if not hasattr(self, name):
                setattr(self, name, getattr(self._entity, entity_name))


class Logs(EntityProxy):

    def __init__(self, client, jobkey):
        super(Logs, self).__init__(_Logs,client, jobkey,
                                   download_api=True, items_api=True)
        self._proxy_methods(['log', 'debug', 'info',
                             'warning', 'warn', 'error'])

    def iter(self, **params):
        if 'offset' in params:
            params['start'] = '%s/%s' % (self._entity._key, params['offset'])
            del params['offset']
        if 'level' in params:
            minlevel = getattr(LogLevel, params.get('level'), None)
            if minlevel is None:
                raise ScrapinghubAPIError(
                    "Unknown log level: %s" % params.get('level'))
            params['filters'] = ['level', '>=', [minlevel]]
        return self._entity.iter_values(**params)


class Items(EntityProxy):

    def __init__(self, client, jobkey):
        super(Items, self).__init__(_Items, client, jobkey,
                                    download_api=True, items_api=True)

    def iter(self, **params):
        if 'offset' in params:
            params['start'] = '%s/%s' % (self._entity.key, params['offset'])
            del params['offset']
        return self._entity.iter_values(**params)


class Requests(EntityProxy):

    def __init__(self, client, jobkey):
        super(Requests, self).__init__(_Requests, client, jobkey,
                                       download_api=True, items_api=True)


class Samples(EntityProxy):

    def __init__(self, client, jobkey):
        super(Samples, self).__init__(_Samples, client, jobkey, items_api=True)


class Collections(EntityProxy):

    def __init__(self, client, jobkey):
        super(Collections, self).__init__(_Collections, client, jobkey,
                                          download_api=True)
        self._proxy_methods([
            'count', 'get', 'set', 'delete', 'create_writer',
            'new_collection', 'new_store', 'new_cached_store',
            'new_versioned_store', 'new_versioned_cached_store',
        ])


def _get_tags_for_update(**kwargs):
    """Helper to check tags changes"""
    params = {}
    for k, v in kwargs.items():
        if not v:
            continue
        if not isinstance(v, list):
            raise ScrapinghubAPIError("Add/remove field value must be a list")
        params[k] = v
    return params
