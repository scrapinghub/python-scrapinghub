
from scrapinghub import Connection
from scrapinghub import HubstorageClient

from scrapinghub.hubstorage.activity import Activity
from scrapinghub.hubstorage.collectionsrt import Collections
from scrapinghub.hubstorage.frontier import Frontier
from scrapinghub.hubstorage.project import Reports
from scrapinghub.hubstorage.project import Settings

from scrapinghub.hubstorage.utils import xauth


class ScrapinghubAPIError(Exception):
    pass


class ScrapinghubClient(object):

    def __init__(self, auth=None, dash_endpoint=None, **kwargs):
        self.connection = Connection(apikey=auth, url=dash_endpoint)
        self.hsclient = HubstorageClient(auth=auth, **kwargs)
        self.projects = Projects(self)

    def get_project(self, projectid):
        return self.projects.get(projectid)

    def get_job(self, jobkey):
        # FIXME solve the splitting more gracefully
        projectid = jobkey.split('/')[0]
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

    def __init__(self, client, jobkey):
        self.client = client
        self.key = jobkey
        self.projectid = jobkey.split('/')[0]

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
