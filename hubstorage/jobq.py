from .resourcetype import ResourceType
from .utils import urlpathjoin


class JobQ(ResourceType):

    resource_type = 'jobq'

    def push(self, spider, **jobparams):
        jobparams['spider'] = spider
        r = self.apipost('push', jl=jobparams)
        return r.next()

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
