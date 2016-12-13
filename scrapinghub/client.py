
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
        self.projectid = projectid

        # sub-resources
        self.spiders = Spiders(client, self.projectid)
        self.jobs = Jobs(client, self.projectid)

        # proxied sub-resources
        hsclient, auth = client.hsclient, client.hsclient.auth
        self.activity = Activity(hsclient, self.projectid, auth=auth)
        self.collections = Collections(hsclient, self.projectid, auth=auth)
        self.frontier = Frontier(hsclient, self.projectid, auth=auth)
        self.settings = Settings(hsclient, self.projectid, auth=auth)
        self.reports = Reports(hsclient, self.projectid, auth=auth)


class Spiders(object):

    def __init__(self, client, projectid):
        self.client = client
        self.projectid = projectid

    def get(self, id=None, name=None):
        if not id:
            if not name:
                raise ScrapinghubAPIError('Please provide spider id or name')
            project = self.client.hsclient.get_project(self.projectid)
            id = project.ids.spider(name)
        return Spider(self.client, self.projectid, spiderid=id)

    def list(self):
        project = self.client.connection[self.projectid]
        return project.spiders()


class Spider(object):

    def __init__(self, client, projectid, spiderid):
        self.client = client
        self.projectid = projectid
        self.spiderid = spiderid
        self.jobs = Jobs(client, self.projectid, self.spiderid)


class Jobs(object):

    def __init__(self, client, projectid, spiderid=None):
        self.client = client
        self.projectid = projectid
        self.spiderid = spiderid

    @property
    def _hsproject(self):
        """Shortcut to hsclient.project for internal uses"""
        return self.client.hsclient.get_project(self.projectid)

    def count(self, **params):
        # FIXME we need spidername here!
        params['spider'] = 'localinfo'
        return next(self._hsproject.jobq.apiget(('count',), params=params))

    def iter(self, **params):
        # FIXME we need spidername here!
        params['spider'] = 'localinfo'
        return self._hsproject.jobq.list(**params)

    def create(self, spidername=None, **params):
        # FIXME we need spidername here as well!
        if not spidername:
            if not self.spiderid:
                raise ScrapinghubAPIError('Please provide spidername')
            spidername = spiderid  # TODO temporary stub
        newjob = self._hsproject.jobq.push(spidername, **params)
        _, projectid, spiderid, jobid = newjob.key.split('/')
        return Job(self.client. self.projectid, spiderid, jobid)

    def get(self, jobkey):
        projectid, spiderid, jobid = jobkey.split('/')
        if projectid != self.projectid:
            raise ScrapinghubAPIError('Please use same project id')
        if self.spiderid and spiderid != self.spiderid:
            raise ScrapinghubAPIError('Please use same spider id')
        return Job(self.client. jobkey)

    def summary(self, **params):
        return self._hsproject.jobq.summary(spiderid=self.spiderid, **params)

    def lastjobsummary(self, **params):
        summ = self._hsproject.spiders.lastjobsummary(self.spiderid, **params)
        # FIXME original lastjobsummary returns a generator, is it ok?
        return list(summ)


class Job(object):

    def __init__(self, client, projectid, spiderid, jobid):
        self.client = client
        self.jobkey = jobkey
        self.projectid, self.spiderid, self.jobid = jobkey.split('/')

    @property
    def _hsproject(self):
        """Shortcut to hsclient.project for internal uses"""
        return self.client.hsclient.get_project(self.projectid)

    def start(self, **params):
        return self._hsproject.jobq.start(self.jobkey, **params)

    def update(self, **params):
        return self._hsproject.jobq.update(self.jobkey, **params)

    def cancel(self):
        self._hsproject.jobq.request_cancel(self.jobkey)

    def finish(self, **params):
        return self._hsproject.jobq.finish(self.jobkey, **params)

    def delete(self, **params):
        return self._hsproject.jobq.delete(self.jobkey, **params)

    def purge(self):
        self.client.hsclient.get_job(self.jobkey).purged()
