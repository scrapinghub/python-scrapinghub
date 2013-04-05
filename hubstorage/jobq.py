from requests.exceptions import HTTPError
from .resourcetype import ResourceType
from .utils import urlpathjoin


class DuplicateJobError(Exception):
    """Raised when a job with same unique is pushed"""


class JobQ(ResourceType):

    resource_type = 'jobq'

    PRIO_LOWEST = 0
    PRIO_LOW = 1
    PRIO_NORMAL = 2
    PRIO_HIGH = 3
    PRIO_HIGHEST = 4

    def push(self, spider, **jobparams):
        jobparams['spider'] = spider
        try:
            for o in self.apipost('push', jl=jobparams):
                return o
        except HTTPError as exc:
            if exc.response.status_code == 409:
                raise DuplicateJobError()
            raise

    def summary(self, _queuename=None, spiderid=None, count=None, start=None):
        path = urlpathjoin(spiderid, 'summary', _queuename)
        r = list(self.apiget(path, params={'count': count, 'start': start}))
        return (r and r[0] or None) if _queuename else r

    def start(self, job=None, botgroup=None):
        if job:
            return self._set_state(job, 'running')

        params = {}
        if botgroup:
            params['botgroup'] = botgroup
        for o in self.apipost('startjob', params=params):
            return o

    def finish(self, job):
        return self._set_state(job, 'finished')

    def delete(self, job):
        return self._set_state(job, 'deleted')

    def _set_state(self, job, state):
        if isinstance(job, dict):
            key = job['key']
        elif hasattr(job, 'key'):
            key = job.key
        else:
            key = job
        r = self.apipost('update', jl={'key': key, 'state': state})
        return r.next()
