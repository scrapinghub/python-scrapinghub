from .job import Job, JobsMeta, Items, Logs
from .jobq import JobQ
from .activity import Activity
from .collectionsrt import Collections


class Project(object):

    def __init__(self, client, projectid, auth=None):
        assert len(projectid.split('/')) == 1, 'projectkey must be just one id: %s' % projectid
        self.projectid = projectid
        self.client = client
        self.auth = auth
        self.jobs = JobsMeta(client, projectid, auth=auth)
        self.items = Items(client, projectid, auth=auth)
        self.logs = Logs(client, projectid, auth=auth)
        self.jobq = JobQ(client, projectid, auth=auth)
        self.activity = Activity(client, projectid, auth=auth)
        self.collections = Collections(client, projectid, auth=auth)

    def get_job(self, _key, *args, **kwargs):
        return self.client.get_job((self.projectid, _key), *args, **kwargs)

    def get_jobs(self, _key=None, **kwargs):
        for metadata in self.jobs.get(_key, meta='_key', **kwargs):
            key = metadata.pop('_key')
            yield self.client.get_job(key, metadata=metadata)

    def new_job(self, spidername, **jobparams):
        data = self.jobq.push(spidername, **jobparams)
        key = data['key']
        auth = (key, data['auth'])
        return Job(key, client=self, auth=auth)
