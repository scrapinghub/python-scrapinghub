import json

from scrapinghub import APIError
from scrapinghub import Connection as _Connection
from scrapinghub import HubstorageClient as _HubstorageClient

from .hubstorage.resourcetype import DownloadableResource
from .hubstorage.resourcetype import ItemsResourceType

# scrapinghub.hubstorage classes to use as-is
from .hubstorage.frontier import Frontier
from .hubstorage.job import JobMeta
from .hubstorage.project import Settings

# scrapinghub.hubstorage proxied classes
from .hubstorage.activity import Activity as _Activity
from .hubstorage.collectionsrt import Collections as _Collections
from .hubstorage.collectionsrt import Collection as _Collection
from .hubstorage.job import Items as _Items
from .hubstorage.job import Logs as _Logs
from .hubstorage.job import Samples as _Samples
from .hubstorage.job import Requests as _Requests

from .utils import DuplicateJobError, NotFound
from .utils import LogLevel
from .utils import get_tags_for_update
from .utils import parse_project_id, parse_job_key
from .utils import proxy_methods
from .utils import wrap_kwargs, wrap_http_errors, wrap_value_too_large


class Connection(_Connection):

    @wrap_http_errors
    def _request(self, *args, **kwargs):
        return super(Connection, self)._request(*args, **kwargs)


class HubstorageClient(_HubstorageClient):

    @wrap_http_errors
    def request(self, *args, **kwargs):
        return super(HubstorageClient, self).request(*args, **kwargs)


class ScrapinghubClient(object):

    def __init__(self, auth=None, dash_endpoint=None, **kwargs):
        self.projects = Projects(self)
        self._connection = Connection(apikey=auth, url=dash_endpoint)
        self._hsclient = HubstorageClient(auth=auth, **kwargs)

    def get_project(self, projectid):
        return self.projects.get(parse_project_id(projectid))

    def get_job(self, jobkey):
        projectid = parse_job_key(jobkey).projectid
        return self.projects.get(projectid).jobs.get(jobkey)

    def close(self, timeout=None):
        self._hsclient.close(timeout=timeout)


class Projects(object):

    def __init__(self, client):
        self._client = client

    def get(self, projectid):
        return Project(self._client, parse_project_id(projectid))

    def list(self):
        return self._client._connection.project_ids()

    def summary(self, **params):
        return self._client._hsclient.projects.jobsummaries(**params)


class Project(object):

    def __init__(self, client, projectid):
        self.id = projectid
        self._client = client

        # sub-resources
        self.jobs = Jobs(client, projectid)
        self.spiders = Spiders(client, projectid)

        # proxied sub-resources
        self.activity = Activity(_Activity, client, projectid)
        self.collections = Collections(_Collections, client, projectid)
        self.frontier = Frontier(client._hsclient, projectid)
        self.settings = Settings(client._hsclient, projectid)


class Spiders(object):

    def __init__(self, client, projectid):
        self.projectid = projectid
        self._client = client

    def get(self, spidername, **params):
        project = self._client._hsclient.get_project(self.projectid)
        spiderid = project.ids.spider(spidername, **params)
        if spiderid is None:
            raise NotFound("Spider {} doesn't exist.".format(spidername))
        return Spider(self._client, self.projectid, spiderid, spidername)

    def list(self):
        project = self._client._connection[self.projectid]
        return project.spiders()


class Spider(object):

    def __init__(self, client, projectid, spiderid, spidername):
        self.projectid = projectid
        self.id = spiderid
        self.name = spidername
        self.jobs = Jobs(client, projectid, self)
        self._client = client

    def update_tags(self, add=None, remove=None):
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'project': self.projectid, 'spider': self.name})
        result = self._client._connection._post('jobs_update', 'json', params)
        return result['count']


class Jobs(object):

    def __init__(self, client, projectid, spider=None):
        self.projectid = projectid
        self.spider = spider
        self._client = client
        self._project = client._hsclient.get_project(projectid)

    def count(self, **params):
        if self.spider:
            params['spider'] = self.spider.name
        return next(self._project.jobq.apiget(('count',), params=params))

    def iter(self, **params):
        """ Iterate over jobs collection for a given set of params.
        FIXME the function returns a list of dicts, not a list of Job's
        """
        if self.spider:
            params['spider'] = self.spider.name
        return self._project.jobq.list(**params)

    def schedule(self, spidername=None, **params):
        if not spidername and not self.spider:
            raise ValueError('Please provide spidername')
        params['project'] = self.projectid
        params['spider'] = spidername or self.spider.name
        if 'job_settings' in params:
            params['job_settings'] = json.dumps(params['job_settings'])
        if 'meta' in params:
            params['meta'] = json.dumps(params['meta'])
        # FIXME improve to have an option to schedule multiple jobs
        try:
            response = self._client._connection._post(
                'schedule', 'json', params)
        except APIError as exc:
            if 'already scheduled' in str(exc):
                raise DuplicateJobError(exc)
            raise
        return Job(self._client, response['jobid'])

    def get(self, jobkey):
        jobkey = parse_job_key(jobkey)
        if jobkey.projectid != self.projectid:
            raise ValueError('Please use same project id')
        if self.spider and jobkey.spiderid != self.spider.id:
            raise ValueError('Please use same spider id')
        return Job(self._client, str(jobkey))

    def summary(self, _queuename=None, **params):
        spiderid = self._extract_spider_id(params)
        return self._project.jobq.summary(
            _queuename, spiderid=spiderid, **params)

    def iter_last(self, **params):
        spiderid = self._extract_spider_id(params)
        return self._project.spiders.lastjobsummary(spiderid, **params)

    def _extract_spider_id(self, params):
        spiderid = params.pop('spiderid', None)
        if not spiderid and self.spider:
            return self.spider.id
        elif spiderid and self.spider and spiderid != self.spider.id:
            raise ValueError('Please use same spider id')
        return spiderid


class Job(object):

    def __init__(self, client, jobkey, metadata=None):
        self.projectid = parse_job_key(jobkey).projectid
        self.key = jobkey

        self._client = client
        self._project = client._hsclient.get_project(self.projectid)
        self._job = client._hsclient.get_job(jobkey)

        # proxied sub-resources
        self.items = Items(_Items, client, jobkey)
        self.logs = Logs(_Logs, client, jobkey)
        self.requests = Requests(_Requests, client, jobkey)
        self.samples = Samples(_Samples, client, jobkey)

        self.metadata = JobMeta(client._hsclient, jobkey, cached=metadata)

    def update_metadata(self, *args, **kwargs):
        self._job.update_metadata(*args, **kwargs)

    def update_tags(self, add=None, remove=None):
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'project': self.projectid, 'job': self.key})
        result = self._client._connection._post('jobs_update', 'json', params)
        return result['count']

    def close_writers(self):
        self._job.close_writers()

    def purge(self):
        self._job.purged()
        self.metadata.expire()

    def start(self, **params):
        return self._project.jobq.start(self, **params)

    def update(self, **params):
        return self._project.jobq.update(self, **params)

    def cancel(self):
        self._project.jobq.request_cancel(self)

    def finish(self, **params):
        return self._project.jobq.finish(self, **params)

    def delete(self, **params):
        return self._project.jobq.delete(self, **params)


class _Proxy(object):
    """A proxy to create a class instance and proxy its methods to origin"""

    def __init__(self, cls, client, key):
        self.key = key
        self._client = client
        self._origin = cls(client._hsclient, key)

        if issubclass(cls, ItemsResourceType):
            self._proxy_methods(['get', 'write', 'flush', 'close',
                                 'stats', ('iter', 'list')])
            # redefine write method to wrap hubstorage.ValueTooLarge error
            origin_method = getattr(self, 'write')
            setattr(self, 'write', wrap_value_too_large(origin_method))

        # DType iter_values() has more priority than IType list()
        if issubclass(cls, DownloadableResource):
            methods = [('iter', 'iter_values'),
                       ('iter_raw_msgpack', 'iter_msgpack'),
                       ('iter_raw_json', 'iter_json')]
            self._proxy_methods(methods)

            # apply_iter_filters is responsible to modify filter params for all
            # iter* calls: should be used only if defined for a child class
            if hasattr(self, '_modify_iter_filters'):
                apply_fn = getattr(self, '_modify_iter_filters')
                for method in [method[0] for method in methods]:
                    wrapped = wrap_kwargs(getattr(self, method), apply_fn)
                    setattr(self, method, wrapped)

    def _proxy_methods(self, methods):
        proxy_methods(self._origin, self, methods)


class Logs(_Proxy):

    def __init__(self, *args, **kwargs):
        super(Logs, self).__init__(*args, **kwargs)
        self._proxy_methods(['log', 'debug', 'info', 'warning', 'warn',
                             'error', 'batch_write_start'])

    def _modify_iter_filters(self, params):
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '{}/{}'.format(self.key, offset)
        level = params.pop('level', None)
        if level:
            minlevel = getattr(LogLevel, level, None)
            if minlevel is None:
                raise ValueError("Unknown log level: {}".format(level))
            params['filter'] = json.dumps(['level', '>=', [minlevel]])
        return params


class Items(_Proxy):

    def _modify_iter_filters(self, params):
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '{}/{}'.format(self.key, offset)
        return params


class Requests(_Proxy):

    def __init__(self, *args, **kwargs):
        super(Requests, self).__init__(*args, **kwargs)
        self._proxy_methods(['add'])


class Samples(_Proxy):
    pass


class Activity(_Proxy):

    def __init__(self, *args, **kwargs):
        super(Activity, self).__init__(*args, **kwargs)
        self._proxy_methods([('iter', 'list')])

    def add(self, *args, **kwargs):
        self._origin.add(*args, **kwargs)

    def post(self, *args, **kwargs):
        self._origin.post(*args, **kwargs)


class Collections(_Proxy):

    def get(self, coltype, colname):
        self._origin._validate_collection(coltype, colname)
        return Collection(self._client, self, coltype, colname)

    def get_store(self, colname):
        return self.get('s', colname)

    def get_cached_store(self, colname):
        return self.get('cs', colname)

    def get_versioned_store(self, colname):
        return self.get('vs', colname)

    def get_versioned_cached_store(self, colname):
        return self.get('vcs', colname)



class Collection(object):

    def __init__(self, client, collections, coltype, colname):
        self._client = client
        self._origin = _Collection(coltype, colname, collections._origin)
        proxy_methods(self._origin, self, [
            'create_writer', 'get', 'set', 'delete', 'count',
            ('iter', 'iter_values'),
            ('iter_raw_json', 'iter_json'),
        ])
