from .job import Job
from .jobq import JobQ
from .activity import Activity
from .collectionsrt import Collections
from .resourcetype import ResourceType


class Project(object):

    def __init__(self, client, projectid, auth=None):
        assert len(str(projectid).split('/')) == 1, 'projectkey must be just one id: %s' % projectid
        self.projectid = projectid
        self.client = client
        self.auth = auth
        self.jobs = Jobs(client, projectid, auth=auth)
        self.items = Items(client, projectid, auth=auth)
        self.logs = Logs(client, projectid, auth=auth)
        self.jobq = JobQ(client, projectid, auth=auth)
        self.activity = Activity(client, projectid, auth=auth)
        self.collections = Collections(client, projectid, auth=auth)

    def get_job(self, _key, *args, **kwargs):
        return self.client.get_job((self.projectid, _key), *args, **kwargs)

    def get_jobs(self, _key=None, **kwargs):
        for metadata in self.jobs.list(_key, meta='_key', **kwargs):
            key = metadata.pop('_key')
            yield self.client.get_job(key, metadata=metadata)

    def new_job(self, spidername, **jobparams):
        data = self.jobq.push(spidername, **jobparams)
        key = data['key']
        auth = (key, data['auth'])
        return Job(self.client, key, auth=auth)


class Jobs(ResourceType):

    resource_type = 'jobs'

    def list(self, _key=None, **params):
        return self.apiget(_key, params=params)

    def summary(self):
        return self.apiget('summary').next()


class Items(ResourceType):

    resource_type = 'items'

    def list(self, _key=None, **params):
        return self.apiget(_key, params=params)


class Logs(ResourceType):

    resource_type = 'logs'

    def list(self, _key=None, **params):
        return self.apiget(_key, params=params)
