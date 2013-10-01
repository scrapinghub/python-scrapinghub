from requests.exceptions import HTTPError
from .resourcetype import ResourceType


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

    def summary(self, _queuename=None, spiderid=None, count=None, start=None, jobmeta=None):
        params = {}
        if count is not None:
            params['count'] = count
        if start is not None:
            params['start'] = start
        if jobmeta is not None:
            params['jobmeta'] = jobmeta

        r = list(self.apiget((spiderid, 'summary', _queuename), params=params))
        return (r and r[0] or None) if _queuename else r

    def start(self, job=None, **start_params):
        """Start a new job

        If a job is passed, it is changed to the started state and metadata
        updated with the start_params. Otherwise the next job is pulled from
        hubstorage, using the start_params which will be saved as metadata.

        If a 'botgroup' parameter is present in start_params, only jobs from
        that botgroup will be started.

        It may take up to a second for a previously added job to be available.
        """
        if job:
            return self._set_state(job, 'running', **start_params)
        for o in self.apipost('startjob', jl=start_params):
            return o

    def request_cancel(self, job):
        """Cancel a running job"""
        self.apipost("%s/cancel" % job.key[job.key.index('/') + 1:])

    def finish(self, job):
        return self._set_state(job, 'finished')

    def delete(self, job):
        return self._set_state(job, 'deleted')

    def _jobkeys(self, job):
        if isinstance(job, list):
            for x in job:
                for k in self._jobkeys(x):
                    yield k
        elif isinstance(job, dict):
            yield job['key']
        elif hasattr(job, 'key'):
            yield job.key
        else:
            yield job

    def _set_state(self, job, state, **extra_args):
        data = [dict(extra_args, key=k, state=state) for k in self._jobkeys(job)]
        return self.apipost('update', jl=data)
