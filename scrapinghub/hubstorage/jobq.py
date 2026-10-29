from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any, Protocol, Union

from requests.exceptions import HTTPError
from .resourcetype import ResourceType


class _HasKey(Protocol):
    @property
    def key(self) -> str: ...


_JobRef = Union[_HasKey, dict[str, Any], str, list["_JobRef"]]


class DuplicateJobError(Exception):
    """Raised when a job with same unique is pushed"""


class JobQ(ResourceType):

    resource_type = 'jobq'

    PRIO_LOWEST = 0
    PRIO_LOW = 1
    PRIO_NORMAL = 2
    PRIO_HIGH = 3
    PRIO_HIGHEST = 4

    def push(self, spider: str, **jobparams: Any) -> Any:
        jobparams['spider'] = spider
        try:
            for o in self.apipost('push', jl=jobparams):
                if 'error' in o:
                    if 'Active job' in o['error']:
                        raise DuplicateJobError(o['error'])
                    raise HTTPError(o['error'])
                return o
        except HTTPError as exc:
            if exc.response and exc.response.status_code == 409:
                raise DuplicateJobError()
            raise

    def jobsummary(self, jobkeys: list[str] | tuple[str, ...],
                   jobmeta: Any) -> Iterator[Any]:
        """Fetch selected job metadata fields for selected jobs."""
        if not isinstance(jobkeys, (list, tuple)):
            raise TypeError("jobkeys must be a list or a tuple")
        return self.apiget(('jobsummary',),
                           params={'key': jobkeys, 'jobmeta': jobmeta})

    def summary(self, _queuename: str | None = None,
                spiderid: str | None = None, count: int | None = None,
                start: int | None = None, jobmeta: Any = None) -> Any:
        params: dict[str, Any] = {}
        if count is not None:
            params['count'] = count
        if start is not None:
            params['start'] = start
        if jobmeta is not None:
            params['jobmeta'] = jobmeta

        r = list(self.apiget((spiderid, 'summary', _queuename), params=params))
        return (r and r[0] or None) if _queuename else r

    def list(self, spider: str | None = None, count: int | None = None,
             stop: str | None = None, state: str | list[str] | None = None,
             has_tag: str | list[str] | None = None,
             lacks_tag: str | list[str] | None = None,
             startts: int | None = None, endts: int | None = None,
             **params: Any) -> Iterator[Any]:
        if 'filter' in params:
            return self._legacy_list_with_filter(params)

        if state is not None:
            params['state'] = state
        if spider is not None:
            params['spider'] = spider
        if count is not None:
            params['count'] = count
        if stop is not None:
            params['stop'] = stop
        if startts is not None:
            params['startts'] = startts
        if endts is not None:
            params['endts'] = endts
        if has_tag is not None:
            params['has_tag'] = has_tag
        if lacks_tag is not None:
            params['lacks_tag'] = lacks_tag
        return self.apiget(('list',), params=params)

    def _legacy_list_with_filter(self, params: dict[str, Any]) -> Iterator[Any]:
        only_finished_outcome = False
        for row in params['filter']:
            field, matchdecider, value = json.loads(row)
            if field == 'tags' and matchdecider == 'haselement':
                params['has_tag'] = value
            if field == 'tags' and matchdecider == 'hasnotelement':
                params['lacks_tag'] = value
            if field == 'state' and matchdecider == '=':
                params['state'] = value
            if field == 'spider' and matchdecider == '=':
                params['spider'] = value
            if field == 'close_reason' and value == ['finished']:
                only_finished_outcome = True
                params.setdefault('jobmeta', []).append('close_reason')

        jobs = self.apiget(('list',), params=params)
        if only_finished_outcome:
            return (x for x in jobs if x.get('close_reason') == 'finished')
        return jobs

    def start(self, job: _JobRef | None = None, **start_params: Any) -> Any:
        """Start a new job

        If a job is passed, it is changed to the started state and metadata
        updated with the start_params. Otherwise the next job is pulled from
        hubstorage, using the start_params which will be saved as metadata.

        If a 'botgroup' parameter is present in start_params, only jobs from
        that botgroup will be started.

        It may take up to a second for a previously added job to be available.
        """
        if job:
            return self.update(job, state='running', **start_params)
        for o in self.apipost('startjob', jl=start_params):
            return o

    def request_cancel(self, job: _HasKey) -> None:
        """Cancel a running job"""
        self.apipost("%s/cancel" % job.key[job.key.index('/') + 1:])

    def finish(self, job: _JobRef, **params: Any) -> Iterator[Any]:
        return self.update(job, state='finished', **params)

    def delete(self, job: _JobRef, **params: Any) -> Iterator[Any]:
        return self.update(job, state='deleted', **params)

    def _jobkeys(self, job: _JobRef) -> Iterator[str]:
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

    def update(self, job: _JobRef, **extra_args: Any) -> Iterator[Any]:
        data = [dict(extra_args, key=k) for k in self._jobkeys(job)]
        return self.apipost('update', jl=data)
