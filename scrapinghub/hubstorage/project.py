from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from .job import Job
from .jobq import JobQ
from .activity import Activity
from .collectionsrt import Collections
from .frontier import Frontier
from .resourcetype import ResourceType, MappingResourceType
from .utils import _Auth, _Part, urlpathjoin, xauth

if TYPE_CHECKING:
    from .client import HubstorageClient


class Project(object):

    def __init__(self, client: HubstorageClient, projectid: _Part,
                 auth: _Auth = None) -> None:
        self.client = client
        self.projectid = urlpathjoin(projectid)
        assert len(self.projectid.split('/')) == 1, \
            'projectkey must be just one id: %s' % projectid
        self.auth = xauth(auth) or client.auth
        self.jobs = Jobs(client, self.projectid, auth=auth)
        self.items = Items(client, self.projectid, auth=auth)
        self.logs = Logs(client, self.projectid, auth=auth)
        self.samples = Samples(client, self.projectid, auth=auth)
        jobq_client = getattr(client, '_jobq_client', client)
        self.jobq = JobQ(jobq_client, self.projectid, auth=auth)
        self.activity = Activity(client, self.projectid, auth=auth)
        self.collections = Collections(client, self.projectid, auth=auth)
        self.frontier = Frontier(client, self.projectid, auth=auth)
        self.ids = Ids(client, self.projectid, auth=auth)
        self.settings = Settings(client, self.projectid, auth=auth)
        self.reports = Reports(client, self.projectid, auth=auth)
        self.spiders = Spiders(client, self.projectid, auth=auth)

    def get_job(self, _key: _Part, *args: Any, **kwargs: Any) -> Job:
        key = urlpathjoin(_key)
        parts = key.split('/')
        jobkey: _Part = key
        if len(parts) == 2:
            jobkey = (self.projectid, key)
        elif len(parts) == 3 and parts[0] == self.projectid:
            pass
        else:
            raise ValueError('Invalid jobkey %s for project %s'
                             % (key, self.projectid))

        kwargs.setdefault('auth', self.auth)
        return self.client.get_job(jobkey, *args, **kwargs)

    def get_jobs(self, **kwargs: Any) -> Iterator[Job]:
        warnings.warn('Method `project.get_jobs()` is deprecated, '
                      'use `project.jobq.list()` instead', Warning)
        for metadata in self.jobq.list(**kwargs):
            key = metadata.pop('key')
            yield self.get_job(key, metadata=metadata)

    def push_job(self, spidername: str, **jobparams: Any) -> Job:
        data = self.jobq.push(spidername, **jobparams)
        key = data['key']
        return Job(self.client, key, auth=self.auth)

    def jobsummary(self, **params: Any) -> Any:
        uri = ('projects', self.projectid, 'jobsummary')
        return next(self.client.root.apiget(uri, auth=self.auth, params=params))


class Jobs(ResourceType):

    resource_type = 'jobs'

    def list(self, _key: _Part | None = None,
             **params: Any) -> Iterator[Any]:
        return self.apiget(_key, params=params)


class Items(ResourceType):

    resource_type = 'items'

    def list(self, _key: _Part | None = None,
             **params: Any) -> Iterator[Any]:
        return self.apiget(_key, params=params)


class Logs(ResourceType):

    resource_type = 'logs'

    def list(self, _key: _Part | None = None,
             **params: Any) -> Iterator[Any]:
        return self.apiget(_key, params=params)


class Samples(ResourceType):

    resource_type = 'samples'

    def list(self, _key: _Part | None = None,
             **params: Any) -> Iterator[Any]:
        return self.apiget(_key, params=params)


class Ids(ResourceType):

    resource_type = 'ids'

    def spider(self, spidername: str, **params: Any) -> Any:
        r = self.apiget(('spider', spidername), params=params)
        return next(r)


class Settings(MappingResourceType):

    resource_type = 'projects'
    key_suffix = 'settings'


class Reports(ResourceType):

    resource_type = 'projects'
    key_suffix = 'reports'


class Spiders(ResourceType):

    resource_type = 'spiders'

    def lastjobsummary(self, spiderid: str | None = None,
                       **params: Any) -> Iterator[Any]:
        return self.apiget((spiderid, 'lastjobsummary'), params=params)
