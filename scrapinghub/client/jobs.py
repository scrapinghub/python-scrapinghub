from __future__ import absolute_import
import json

from ..hubstorage.job import JobMeta as _JobMeta
from ..hubstorage.job import Items as _Items
from ..hubstorage.job import Logs as _Logs
from ..hubstorage.job import Samples as _Samples
from ..hubstorage.job import Requests as _Requests

from .items import Items
from .logs import Logs
from .requests import Requests
from .samples import Samples
from .exceptions import NotFound, InvalidUsage, DuplicateJobError
from .utils import _MappingProxy, get_tags_for_update, parse_job_key


class Jobs(object):
    """Class representing a collection of jobs for a project/spider.

    Not a public constructor: use :class:`Project` instance or :class:`Spider`
    instance to get a :class:`Jobs` instance. See :attr:`Project.jobs` and
    :attr:`Spider.jobs` attributes.

    :ivar projectid: an integer project id.
    :ivar spider: :class:`Spider` object if defined.

    Usage::

        >>> project.jobs
        <scrapinghub.client.Jobs at 0x10477f0b8>
        >>> spider = project.spiders.get('spider1')
        >>> spider.jobs
        <scrapinghub.client.Jobs at 0x104767e80>
    """

    def __init__(self, client, projectid, spider=None):
        self.projectid = projectid
        self.spider = spider
        self._client = client
        self._project = client._hsclient.get_project(projectid)

    def count(self, **params):
        """Count jobs for a given set of parameters.

        :param \*\*params: (optional) a set of filters to apply when counting
            jobs (e.g. spider, state, has_tag, lacks_tag, startts and endts).
        :return: jobs count.

        Usage::

            >>> spider = project.spiders.get('spider1')
            >>> spider.jobs.count()
            5
            >>> project.jobs.count(spider='spider2', state='finished')
            2
        """
        if self.spider:
            params['spider'] = self.spider.name
        return next(self._project.jobq.apiget(('count',), params=params))

    def iter(self, **params):
        """Iterate over jobs collection for a given set of params.

        :param \*\*params: (optional) a set of filters to apply when counting
            jobs (e.g. spider, state, has_tag, lacks_tag, startts and endts).
        :return: a generator object over a list of dictionaries of jobs summary
            for a given filter params.

        Usage:

        - retrieve all jobs for a spider::

            >>> spider.jobs.iter()
            <generator object jldecode at 0x1049bd570>

        - get all job keys for a spider::

            >>> jobs_summary = spider.jobs.iter()
            >>> [job['key'] for job in jobs_summary]
            ['123/1/3', '123/1/2', '123/1/1']

        - job summary fieldset is less detailed than job.metadata but contains
        few new fields as well. Additional fields can be requested using
        ``jobmeta`` parameter. If it's used, then it's up to the user to list
        all the required fields, so only few default fields would be added
        except requested ones::

            >>> jobs_summary = project.jobs.iter(jobmeta=['scheduled_by', ])

        - by default :meth:`Jobs.iter` returns maximum last 1000 results.
        Pagination is available using start parameter::

            >>> jobs_summary = spider.jobs.iter(start=1000)

        - get jobs filtered by tags (list of tags has ``OR`` power)::

            >>> jobs_summary = project.jobs.iter(
            ...     has_tag=['new', 'verified'], lacks_tag='obsolete')

        - get certain number of last finished jobs per some spider::

            >>> jobs_summary = project.jobs.iter(
            ...     spider='spider2', state='finished', count=3)
        """
        if self.spider:
            params['spider'] = self.spider.name
        return self._project.jobq.list(**params)

    def list(self, **params):
        """Convenient shortcut to list iter results.

        Please note that list() method can use a lot of memory and for a large
        amount of jobs it's recommended to iterate through it via iter()
        method (all params and available filters are same for both methods).

        """
        return list(self.iter(**params))

    def schedule(self, spidername=None, **params):
        """Schedule a new job and returns its jobkey.

        :param spidername: a spider name string
            (not needed if job is scheduled via :attr:`Spider.jobs`).
        :param \*\*params: (optional) additional keyword args.
        :return: a jobkey string pointing to the new job.

        Usage::

            >>> project.schedule('spider1', arg1='val1')
            '123/1/1'
        """
        if not spidername and not self.spider:
            raise ValueError('Please provide spidername')
        params['project'] = self.projectid
        params['spider'] = spidername or self.spider.name
        spider_args = params.pop('spider_args', None)
        if spider_args:
            if not isinstance(spider_args, dict):
                raise ValueError("spider_args should be a dictionary")
            cleaned_args = {k: v for k, v in spider_args.items()
                            if k not in params}
            params.update(cleaned_args)
        if 'job_settings' in params:
            params['job_settings'] = json.dumps(params['job_settings'])
        if 'meta' in params:
            params['meta'] = json.dumps(params['meta'])
        # FIXME improve to schedule multiple jobs
        try:
            response = self._client._connection._post(
                'schedule', 'json', params)
        except InvalidUsage as exc:
            if 'already scheduled' in str(exc):
                raise DuplicateJobError(exc)
            raise
        return Job(self._client, response['jobid'])

    def get(self, jobkey):
        """Get a Job with a given jobkey.

        :param jobkey: a string job key.

        jobkey's project component should match the project used to get
        :class:`Jobs` instance, and jobkey's spider component should match
        the spider (if :attr:`Spider.jobs` was used).

        :return: :class:`Job` object.
        :rtype: scrapinghub.client.Job.

        Usage::

            >>> job = project.jobs.get('123/1/2')
            >>> job.key
            '123/1/2'
        """
        jobkey = parse_job_key(jobkey)
        if jobkey.projectid != self.projectid:
            raise ValueError('Please use same project id')
        if self.spider and jobkey.spiderid != self.spider._id:
            raise ValueError('Please use same spider id')
        return Job(self._client, str(jobkey))

    def summary(self, _queuename=None, **params):
        """Get jobs summary (optionally by state).

        :param _queuename: (optional) a string state to filter jobs.
        :param \*\*params: (optional) additional keyword args.
        :return: a generator object over a list of dictionaries of jobs summary
            for a given filter params grouped by job state.

        Usage::

            >>> spider.jobs.summary()
            [{'count': 0, 'name': 'pending', 'summary': []},
             {'count': 0, 'name': 'running', 'summary': []},
             {'count': 5, 'name': 'finished', 'summary': [...]}

            >>> project.jobs.summary('pending')
            {'count': 0, 'name': 'pending', 'summary': []}
        """
        spiderid = self._extract_spider_id(params)
        return self._project.jobq.summary(
            _queuename, spiderid=spiderid, **params)

    def iter_last(self, **params):
        """Iterate through last jobs for each spider.

        :param \*\*params: (optional) keyword arguments to filter jobs.
        :return: a generator object over a list of dictionaries of jobs summary
            for a given filter params.

        Usage:

        - get all last job summaries for a project::

            >>> project.jobs.iter_last()
            <generator object jldecode at 0x1048a95c8>

        - get last job summary for a a spider::

            >>> list(spider.jobs.iter_last())
            [{'close_reason': 'success',
              'elapsed': 3062444,
              'errors': 1,
              'finished_time': 1482911633089,
              'key': '123/1/3',
              'logs': 8,
              'pending_time': 1482911596566,
              'running_time': 1482911598909,
              'spider': 'spider1',
              'state': 'finished',
              'ts': 1482911615830,
              'version': 'some-version'}]
        """
        spiderid = self._extract_spider_id(params)
        return self._project.spiders.lastjobsummary(spiderid, **params)

    def _extract_spider_id(self, params):
        spiderid = params.pop('spiderid', None)
        if not spiderid and self.spider:
            return self.spider._id
        elif spiderid and self.spider and str(spiderid) != self.spider._id:
            raise ValueError('Please use same spider id')
        return str(spiderid) if spiderid else None

    def update_tags(self, add=None, remove=None, spidername=None):
        """Update tags for all existing spider jobs.

        :param add: (optional) list of tags to add to selected jobs.
        :param remove: (optional) list of tags to remove from selected jobs.
        :param spidername: spider name, must if used with :attr:`Project.jobs`.

        It's not allowed to update tags for all project jobs, so spider must be
        specified (it's done implicitly when using :attr:`Spider.jobs`, or you
        have to specify ``spidername`` param when using :attr:`Project.jobs`).

        :return: amount of jobs that were updated.

        Usage:

        - mark all spider jobs with tag ``consumed``::

            >>> spider = project.spiders.get('spider1')
            >>> spider.jobs.update_tags(add=['consumed'])
            5

        - remove existing tag ``existing`` for all spider jobs::

            >>> project.jobs.update_tags(
            ...     remove=['existing'], spidername='spider2')
            2
        """
        spidername = spidername or (self.spider.name if self.spider else None)
        if not spidername:
            raise ValueError('Please provide spidername')
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'project': self.projectid, 'spider': spidername})
        result = self._client._connection._post('jobs_update', 'json', params)
        return result['count']


class Job(object):
    """Class representing a job object.

    Not a public constructor: use :class:`ScrapinghubClient` instance or
    :class:`Jobs` instance to get a :class:`Job` instance. See
    :meth:`ScrapinghubClient.get_job` and :meth:`Jobs.get` methods.

    :ivar projectid: in integer project id.
    :ivar key: a job key.
    :ivar items: :class:`Items` resource object.
    :ivar logs: :class:`Logs` resource object.
    :ivar requests: :class:`Requests` resource object.
    :ivar samples: :class:`Samples` resource object.
    :ivar metadata: :class:`Metadata` resource.

    Usage::

        >>> job = project.job('123/1/2')
        >>> job.key
        '123/1/2'
        >>> job.metadata.get('state')
        'finished'
    """
    def __init__(self, client, jobkey):
        self.projectid = parse_job_key(jobkey).projectid
        self.key = jobkey

        self._client = client
        self._project = client._hsclient.get_project(self.projectid)
        self._job = client._hsclient.get_job(jobkey)

        # proxied sub-resources
        self.items = Items(_Items, client, jobkey)
        self.logs = Logs(_Logs, client, jobkey)
        self.requests = Requests(_Requests, client, jobkey)
        self.samples = Samples(_Samples, client, jobkey)

        self.metadata = JobMeta(_JobMeta, client, jobkey)

    def update_tags(self, add=None, remove=None):
        """Partially update job tags.

        It provides a convenient way to mark specific jobs (for better search,
        postprocessing etc).

        :param add: (optional) list of tags to add
        :param remove: (optional) list of tags to remove
        :return: amount of jobs that were updated

        Usage: to mark a job with tag ``consumed``::

            >>> job.update_tags(add=['consumed'])
        """
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        params.update({'project': self.projectid, 'job': self.key})
        result = self._client._connection._post('jobs_update', 'json', params)
        return result['count']

    def close_writers(self):
        """Stop job batch writers threads gracefully.

        Called on :meth:`ScrapinghubClient.close` method.
        """
        self._job.close_writers()

    def start(self, **params):
        """Move job to running state.

        :param \*\*params: (optional) keyword meta parameters to update
        :return: a previous string job state

        Usage::

            >>> job.start()
            'pending'
        """
        return self.update(state='running', **params)

    def finish(self, **params):
        """Move running job to finished state.

        :param \*\*params: (optional) keyword meta parameters to update
        :return: a previous string job state

        Usage::

            >>> job.finish()
            'running'
        """
        return self.update(state='finished', **params)

    def delete(self, **params):
        """Mark finished job for deletion.

        :param \*\*params: (optional) keyword meta parameters to update
        :return: a previous string job state

        Usage::

            >>> job.delete()
            'finished'
        """
        return self.update(state='deleted', **params)

    def update(self, **params):
        """Update job state.

        :param \*\*params: (optional) keyword meta parameters to update
        :return: a previous string job state

        Usage::

            >>> job.update(state='finished')
            'running'
        """
        try:
            job = next(self._project.jobq.update(self, **params))
            return job['prevstate']
        except StopIteration:
            raise NotFound("Job {} doesn't exist".format(self.key))

    def cancel(self):
        """Schedule a running job for cancellation.

        Usage::

            >>> job.cancel()
            >>> job.metadata.get('cancelled_by')
            'John'
        """
        self._project.jobq.request_cancel(self)

    def purge(self):
        """Delete job and expire its local metadata.

        Usage::

            >>> job.purge()
            >>> job.metadata.get('state')
            'deleted'
        """
        self.delete()
        self.metadata.expire()


class JobMeta(_MappingProxy):
    """Class representing job metadata.

    Not a public constructor: use :class:`Job` instance to get a
    :class:`Jobmeta` instance. See :attr:`Job.metadata` attribute.

    Usage::

    - get job metadata instance

        >>> job.metadata
        <scrapinghub.client.jobs.JobMeta at 0x10494f198>

    - iterate through job metadata

        >>> job.metadata.iter()
        <dict_itemiterator at 0x104adbd18>

    - list job metadata

        >>> job.metadata.list()
        [('project', 123), ('units', 1), ('state', 'finished'), ...]

    - get meta field value by name

        >>> job.metadata.get('version')
        'test'

    - update job meta field value (some meta fields are read-only)

        >>> job.metadata.set('my-meta', 'test')

    - update multiple meta fields at once

        >>> job.metadata.set({'my-meta1': 'test1', 'my-meta2': 'test2})

    - delete meta field by name

        >>> job.metadata.delete('my-meta')
    """
