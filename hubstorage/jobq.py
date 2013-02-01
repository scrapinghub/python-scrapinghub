from .resourcetype import ResourceType
from .utils import urlpathjoin


class JobQ(ResourceType):

    resource_type = 'jobq'

    PRIO_LOWEST = 0
    PRIO_LOW = 1
    PRIO_NORMAL = 2
    PRIO_HIGH = 3
    PRIO_HIGHEST = 4

    def push(self, spider, **jobparams):
        jobparams['spider'] = spider
        for o in self.apipost('push', jl=jobparams):
            return o

    def poll(self):
        # XXX: This is completely unsafe call that simulates
        # polling from jobq "safely"
        # It is obviously doing unnecessary wrap/unwraps of auth
        # because summary doesn't contain auth token and caller
        # expects a return value similar to push()
        summary = self.summary('pending')
        if summary and summary['summary']:
            jobkey = summary['summary'][-1]['key']
            job = self.client.get_job(jobkey, auth=self.auth)
            auth = job.metadata.get('auth')
            self.start(jobkey)
            return {'key': jobkey, 'auth': auth}

    def startjob(self):
        for o in self.apipost('startjob'):
            return o

    def summary(self, _queuename=None, spiderid=None):
        path = urlpathjoin(spiderid, 'summary', _queuename)
        r = list(self.apiget(path))
        return (r and r[0] or None) if _queuename else r

    def _set_state(self, job, state):
        if isinstance(job, dict):
            key = job['key']
        elif hasattr(job, 'key'):
            key = job.key
        else:
            key = job
        r = self.apipost('update', jl={'key': key, 'state': state})
        return r.next()

    def start(self, job):
        return self._set_state(job, 'running')

    def finish(self, job):
        return self._set_state(job, 'finished')

    def delete(self, job):
        return self._set_state(job, 'deleted')
