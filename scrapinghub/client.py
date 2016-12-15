
from scrapinghub import Connection
from scrapinghub import HubstorageClient

from scrapinghub.hubstorage.activity import Activity
from scrapinghub.hubstorage.collectionsrt import Collections
from scrapinghub.hubstorage.frontier import Frontier
from scrapinghub.hubstorage.project import Reports
from scrapinghub.hubstorage.project import Settings
from scrapinghub.hubstorage.job import JobMeta
from scrapinghub.hubstorage.job import Samples as _Samples

from scrapinghub.hubstorage.job import Items as _Items
from scrapinghub.hubstorage.job import Logs as _Logs
from scrapinghub.hubstorage.job import Requests as _Requests


class ScrapinghubAPIError(Exception):
    pass


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
        return Project(self.client, projectid)

    def list(self):
        return self.client.connection.project_ids()

    def summary(self, **params):
        return self.client.hsclient.projects.jobsummaries(**params)


class Project(object):

    def __init__(self, client, projectid):
        self.client = client
        self.id = projectid

        # sub-resources
        self.spiders = Spiders(client, self.id)
        self.jobs = Jobs(client, self.id)

        # proxied sub-resources
        # FIXME providing client.hsclient.auth is not necessary!
        hsclient, auth = client.hsclient, client.hsclient.auth
        self.activity = Activity(hsclient, self.id, auth=auth)
        self.collections = Collections(hsclient, self.id, auth=auth)
        self.frontier = Frontier(hsclient, self.id, auth=auth)
        self.settings = Settings(hsclient, self.id, auth=auth)
        self.reports = Reports(hsclient, self.id, auth=auth)


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
        self.jobs = Jobs(client, self.projectid, self)


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
        if self.spider:
            params['spider'] = self.spider.name
        return self._hsproject.jobq.list(**params)

    def create(self, spidername=None, **params):
        if not spidername and not self.spider:
            raise ScrapinghubAPIError('Please provide spidername')
        # FIXME the method should use Dash endpoint, not JobQ
        jobq = self._hsproject.jobq
        newjob = jobq.push(spidername or self.spider.name, **params)
        return Job(self.client, newjob['key'])

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
        self.items = Items(self.client, self.key)
        self.logs = Logs(self.client, self.key)
        self.requests = Requests(self.client, self.key)
        self.samples = Samples(self.client, self.key)

        hsclient, auth = client.hsclient, client.hsclient.auth
        self.metadata = JobMeta(hsclient, self.key, auth, cached=metadata)

    def update_metadata(self, *args, **kwargs):
        self.client.hsclient.get_job(self.key).update_metadata(*args, **kwargs)

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


class ResourceTypes(object):
    """Enum to keep different resource types"""
    DOWNLOADABLE_TYPE = 1
    ITEMS_TYPE = 2
    MAPPING_TYPE = 3


class EntityProxy(object):

    def __init__(self, cls, cls_types, client, key):
        self.client = client
        self.key = key
        self._entity = cls(client.hsclient, key)
        proxy_methods = {}
        if ResourceTypes.ITEMS_TYPE in cls_types:
            proxy_methods.update({
                'iter': 'list', 'get': 'get',
                'write': 'write', 'flush': 'flush',
                'close': 'close', 'stats': 'stats',
            })
        # DType iter_values() has more priority than IType list()
        if ResourceTypes.DOWNLOADABLE_TYPE in cls_types:
            proxy_methods.update({
                'iter': 'iter_values',
                'iter_raw_msgpack': 'iter_msgpack',
                'iter_raw_json': 'iter_json',
            })
        self._proxy_methods(proxy_methods)

    def _proxy_methods(self, methods):
        for name, entity_name in methods.items():
            # save from redefining attribute twice and maintain a proper order
            if not hasattr(self, name):
                setattr(self, name, getattr(self._entity, entity_name))


class Logs(EntityProxy):

    def __init__(self, client, jobkey):
        cls_types = [ResourceTypes.DOWNLOADABLE_TYPE, ResourceTypes.ITEMS_TYPE]
        super(Logs, self).__init__(_Logs, cls_types, client, jobkey)

        # inherite main logs methods
        self.log = self._entity.log
        self.debug = self._entity.debug
        self.info = self._entity.info
        self.warn = self._entity.warn
        self.warning = self._entity.warning
        self.error = self._entity.error

    def iter(self, **params):
        if 'offset' in params:
            params['start'] = '%s/%s' % (self._entity._key, params['offset'])
            del params['offset']
        if 'level' in params:
            minlevel = getattr(Logs, params.get('level'), None)
            if minlevel is None:
                raise ScrapinghubAPIError(
                    "Unknown log level: %s" % params.get('level'))
            params['filters'] = ['level', '>=', [minlevel]]
        return self._entity.iter_values(**params)


class Items(EntityProxy):

    def __init__(self, client, jobkey):
        cls_types = [ResourceTypes.DOWNLOADABLE_TYPE, ResourceTypes.ITEMS_TYPE]
        super(Items, self).__init__(_Items, cls_types, client, jobkey)

    def iter(self, **params):
        if 'offset' in params:
            params['start'] = '%s/%s' % (self._entity.key, params['offset'])
            del params['offset']
        return self._entity.iter_values(**params)


class Requests(EntityProxy):

    def __init__(self, client, jobkey):
        cls_types = [ResourceTypes.DOWNLOADABLE_TYPE, ResourceTypes.ITEMS_TYPE]
        super(Requests, self).__init__(_Requests, cls_types, client, jobkey)


class Samples(EntityProxy):

    def __init__(self, client, jobkey):
        cls_types = [ResourceTypes.ITEMS_TYPE]
        super(Samples, self).__init__(_Samples, cls_types, client, jobkey)
