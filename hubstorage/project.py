from .job import Job
from .jobq import JobQ
from .activity import Activity
from .collectionsrt import Collections
from .resourcetype import ResourceType
from .utils import urlpathjoin, xauth


class Project(object):

    def __init__(self, client, projectid, auth=None):
        self.client = client
        self.projectid = urlpathjoin(projectid)
        assert len(self.projectid.split('/')) == 1, 'projectkey must be just one id: %s' % projectid
        self.auth = xauth(auth) or client.auth
        self.jobs = Jobs(client, self.projectid, auth=auth)
        self.items = Items(client, self.projectid, auth=auth)
        self.logs = Logs(client, self.projectid, auth=auth)
        self.samples = Samples(client, self.projectid, auth=auth)
        self.jobq = JobQ(client, self.projectid, auth=auth)
        self.activity = Activity(client, self.projectid, auth=auth)
        self.collections = Collections(client, self.projectid, auth=auth)

    def get_job(self, _key, *args, **kwargs):
        key = urlpathjoin(_key)
        parts = key.split('/')
        if len(parts) == 2:
            key = (self.projectid, key)
        elif len(parts) == 3 and parts[0] == self.projectid:
            pass
        else:
            raise ValueError('Invalid jobkey %s for project %s' % (key, self.projectid))

        kwargs.setdefault('auth', self.auth)
        return self.client.get_job(key, *args, **kwargs)

    def get_jobs(self, _key=None, **kwargs):
        for metadata in self.jobs.list(_key, meta='_key', **kwargs):
            key = metadata.pop('_key')
            yield self.client.get_job(key, metadata=metadata)

    def new_job(self, spidername, **jobparams):
        data = self.jobq.push(spidername, **jobparams)
        key = data['key']
        auth = (key, data['auth'])
        return Job(self.client, key, jobauth=auth)


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


class Samples(ResourceType):

    resource_type = 'samples'

    def list(self, _key=None, **params):
        return self.apiget(_key, params=params)
