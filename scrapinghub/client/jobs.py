from __future__ import absolute_import

from ..hubstorage.job import JobMeta as _JobMeta
from ..hubstorage.job import Items as _Items
from ..hubstorage.job import Logs as _Logs
from ..hubstorage.job import Samples as _Samples
from ..hubstorage.job import Requests as _Requests

from .items import Items
from .logs import Logs
from .requests import Requests
from .samples import Samples
from .exceptions import NotFound, BadRequest, DuplicateJobError
from .proxy import _MappingProxy
from .utils import get_tags_for_update, parse_job_key, update_kwargs


class Jobs(object):
    """Class representing a collection of jobs for a project/spider.

    Not a public constructor: use :class:`~scrapinghub.client.projects.Project`
    instance or :class:`~scrapinghub.client.spiders.Spider` instance to get
    a :class:`Jobs` instance. See :attr:`scrapinghub.client.projects.Project.jobs`
    and :attr:`scrapinghub.client.spiders.Spider.jobs` attributes.

    :ivar project_id: a string project id.
    :ivar spider: :class:`~scrapinghub.client.spiders.Spider` object if defined.

    Usage::

        >>> project.jobs
        <scrapinghub.client.jobs.Jobs at 0x10477f0b8>
        >>> spider = project.spiders.get('spider1')
        >>> spider.jobs
        <scrapinghub.client.jobs.Jobs at 0x104767e80>
    """

    def __init__(self, client, project_id, spider=None):
        self.project_id = project_id
        self.spider = spider
        self._client = client
        self._project = client._hsclient.get_project(project_id)

    def count(self, spider=None, state=None, has_tag=None, lacks_tag=None,
              startts=None, endts=None, **params):
        """Count jobs with a given set of filters.

        :param spider: (optional) filter by spider name.
        :param state: (optional) a job state, a string or a list of strings.
        :param has_tag: (optional) filter results by existing tag(s), a string
            or a list of strings.
        :param lacks_tag: (optional) filter results by missing tag(s), a string
            or a list of strings.
        :param startts: (optional) UNIX timestamp at which to begin results,
            in millisecons.
        :param endts: (optional) UNIX timestamp at which to end results,
            in millisecons.
        :param \*\*params: (optional) other filter params.

        :return: jobs count.
        :rtype: :class:`int`

        The endpoint used by the method counts only finished jobs by default,
        use ``state`` parameter to count jobs in other states.

        Usage::

            >>> spider = project.spiders.get('spider1')
            >>> spider.jobs.count()
            5
            >>> project.jobs.count(spider='spider2', state='finished')
            2
        """
        update_kwargs(params, spider=spider, state=state, has_tag=has_tag,
                      lacks_tag=lacks_tag, startts=startts, endts=endts)
        if self.spider:
            params['spider'] = self.spider.name
        return next(self._project.jobq.apiget(('count',), params=params))

    def iter(self, count=None, start=None, spider=None, state=None,
             has_tag=None, lacks_tag=None, startts=None, endts=None,
             meta=None, **params):
        """Iterate over jobs collection for a given set of params.

        :param count: (optional) limit amount of returned jobs.
        :param start: (optional) number of jobs to skip in the beginning.
        :param spider: (optional) filter by spider name.
        :param state: (optional) a job state, a string or a list of strings.
        :param has_tag: (optional) filter results by existing tag(s), a string
            or a list of strings.
        :param lacks_tag: (optional) filter results by missing tag(s), a string
            or a list of strings.
        :param startts: (optional) UNIX timestamp at which to begin results,
            in millisecons.
        :param endts: (optional) UNIX timestamp at which to end results,
            in millisecons.
        :param meta: (optional) request for additional fields, a single
            field name or a list of field names to return.
        :param \*\*params: (optional) other filter params.

        :return: a generator object over a list of dictionaries of jobs summary
            for a given filter params.
        :rtype: :class:`types.GeneratorType[dict]`

        The endpoint used by the method returns only finished jobs by default,
        use ``state`` parameter to return jobs in other states.

        Usage:

        - retrieve all jobs for a spider::

            >>> spider.jobs.iter()
            <generator object jldecode at 0x1049bd570>

        - get all job keys for a spider::

            >>> jobs_summary = spider.jobs.iter()
            >>> [job['key'] for job in jobs_summary]
            ['123/1/3', '123/1/2', '123/1/1']

        - job summary fieldset is less detailed than :class:`JobMeta` but
          contains a few new fields as well. Additional fields can be requested
          using ``meta`` parameter. If it's used, then it's up to the user to
          list all the required fields, so only few default fields would be
          added except requested ones::

            >>> jobs_summary = project.jobs.iter(meta=['scheduled_by', ])

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
        update_kwargs(params, count=count, start=start, jobmeta=meta,
                      spider=spider, state=state, has_tag=has_tag,
                      lacks_tag=lacks_tag, startts=startts, endts=endts)
        if self.spider:
            params['spider'] = self.spider.name
        return self._project.jobq.list(**params)

    def list(self, count=None, start=None, spider=None, state=None,
             has_tag=None, lacks_tag=None, startts=None, endts=None,
             meta=None, **params):
        """Convenient shortcut to list iter results.

        :param count: (optional) limit amount of returned jobs.
        :param start: (optional) number of jobs to skip in the beginning.
        :param spider: (optional) filter by spider name.
        :param state: (optional) a job state, a string or a list of strings.
        :param has_tag: (optional) filter results by existing tag(s), a string
            or a list of strings.
        :param lacks_tag: (optional) filter results by missing tag(s), a string
            or a list of strings.
        :param startts: (optional) UNIX timestamp at which to begin results,
            in millisecons.
        :param endts: (optional) UNIX timestamp at which to end results,
            in millisecons.
        :param meta: (optional) request for additional fields, a single
            field name or a list of field names to return.
        :param \*\*params: (optional) other filter params.

        :return: list of dictionaries of jobs summary for a given filter params.
        :rtype: :class:`list[dict]`

        The endpoint used by the method returns only finished jobs by default,
        use ``state`` parameter to return jobs in other states.

        Please note that :meth:`list` can use a lot of memory and for a large
        amount of logs it's recommended to iterate through it via :meth:`iter`
        method (all params and available filters are same for both methods).
        """
        # FIXME we double-check the params here, is there a better way?
        # Simpler way would be to keep **params only here and point to iter(),
        # but then we loose hinting kwargs for list() method.
        update_kwargs(params, count=count, start=start, meta=meta,
                      spider=spider, state=state, has_tag=has_tag,
                      lacks_tag=lacks_tag, startts=startts, endts=endts)
        return list(self.iter(**params))

    def run(self, spider=None, units=None, priority=None, meta=None,
            add_tag=None, job_args=None, job_settings=None, cmd_args=None,
            environment=None, **params):
        """Schedule a new job and returns its job key.

        :param spider: a spider name string
            (not needed if job is scheduled via :attr:`Spider.jobs`).
        :param units: (optional) amount of units for the job.
        :param priority: (optional) integer priority value.
        :param meta: (optional) a dictionary with metadata.
        :param add_tag: (optional) a string tag or a list of tags to add.
        :param job_args: (optional) a dictionary with job arguments.
        :param job_settings: (optional) a dictionary with job settings.
        :param cmd_args: (optional) a string with script command args.
        :param environment: (option) a dictionary with custom environment
        :param \*\*params: (optional) additional keyword args.

        :return: a job instance, representing the scheduled job.
        :rtype: :class:`Job`

        Usage::

            >>> job = project.jobs.run('spider1', job_args={'arg1': 'val1'})
            >>> job
            <scrapinghub.client.jobs.Job at 0x7fcb7c01df60>
            >>> job.key
            '123/1/1'
        """
        if not spider and not self.spider:
            raise ValueError('Please provide `spider` name')
        if job_args:
            if not isinstance(job_args, dict):
                raise ValueError("job_args should be a dictionary")
            cleaned_args = {k: v for k, v in job_args.items()
                            if k not in params}
            params.update(cleaned_args)
        if environment and not isinstance(environment, dict):
            raise ValueError("environment should be a dictionary")

        params['project'] = self.project_id
        params['spider'] = spider or self.spider.name

        update_kwargs(params, units=units, priority=priority, add_tag=add_tag,
                      cmd_args=cmd_args, job_settings=job_settings, meta=meta,
                      environment=environment)

        # FIXME improve to run multiple jobs
        try:
            response = self._client._connection._post('run', 'json', params)
        except BadRequest as exc:
            if 'already scheduled' in str(exc):
                raise DuplicateJobError(exc)
            raise
        return Job(self._client, response['jobid'])

    def get(self, job_key):
        """Get a :class:`Job` with a given job_key.

        :param job_key: a string job key.

        job_key's project component should match the project used to get
        :class:`Jobs` instance, and job_key's spider component should match
        the spider (if :class:`~scrapinghub.client.spiders.Spider` was used
        to get :class:`Jobs` instance).

        :return: a job object.
        :rtype: :class:`Job`

        Usage::

            >>> job = project.jobs.get('123/1/2')
            >>> job.key
            '123/1/2'
        """
        job_key = parse_job_key(job_key)
        if job_key.project_id != self.project_id:
            raise ValueError('Please use same project id')
        if self.spider and job_key.spider_id != self.spider._id:
            raise ValueError('Please use same spider id')
        return Job(self._client, str(job_key))

    def summary(self, state=None, spider=None, **params):
        """Get jobs summary (optionally by state).

        :param state: (optional) a string state to filter jobs.
        :param spider: (optional) a spider name (not needed if instantiated
            with :class:`~scrapinghub.client.spiders.Spider`).
        :param \*\*params: (optional) additional keyword args.
        :return: a list of dictionaries of jobs summary
            for a given filter params grouped by job state.
        :rtype: :class:`list[dict]`

        Usage::

            >>> spider.jobs.summary()
            [{'count': 0, 'name': 'pending', 'summary': []},
             {'count': 0, 'name': 'running', 'summary': []},
             {'count': 5, 'name': 'finished', 'summary': [...]}

            >>> project.jobs.summary('pending')
            {'count': 0, 'name': 'pending', 'summary': []}
        """
        spider_id = self._extract_spider_id(spider)
        return self._project.jobq.summary(
            state, spiderid=spider_id, **params)

    def iter_last(self, start=None, start_after=None, count=None,
                  spider=None, **params):
        """Iterate through last jobs for each spider.

        :param start: (optional)
        :param start_after: (optional)
        :param count: (optional)
        :param spider: (optional) a spider name (not needed if instantiated
            with :class:`~scrapinghub.client.spiders.Spider`).
        :param \*\*params: (optional) additional keyword args.
        :return: a generator object over a list of dictionaries of jobs summary
            for a given filter params.
        :rtype: :class:`types.GeneratorType[dict]`

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
        spider_id = self._extract_spider_id(spider)
        update_kwargs(params, start=start, startafter=start_after, count=count)
        return self._project.spiders.lastjobsummary(spider_id, **params)

    def _extract_spider_id(self, spider):
        if not spider and self.spider:
            return self.spider._id
        if spider:
            project = self._client.get_project(self.project_id)
            spider_id = project.spiders.get(spider)._id
            if self.spider and spider_id != self.spider._id:
                raise ValueError('Please use same spider')
            return spider_id
        return None

    def update_tags(self, add=None, remove=None, spider=None):
        """Update tags for all existing spider jobs.

        :param add: (optional) list of tags to add to selected jobs.
        :param remove: (optional) list of tags to remove from selected jobs.
        :param spider: (optional) spider name, must if used with
            :attr:`Project.jobs`.

        It's not allowed to update tags for all project jobs, so spider must be
        specified (it's done implicitly when using :attr:`Spider.jobs`, or you
        have to specify ``spider`` param when using :attr:`Project.jobs`).

        :return: amount of jobs that were updated.
        :rtype: :class:`int`

        Usage:

        - mark all spider jobs with tag ``consumed``::

            >>> spider = project.spiders.get('spider1')
            >>> spider.jobs.update_tags(add=['consumed'])
            5

        - remove existing tag ``existing`` for all spider jobs::

            >>> project.jobs.update_tags(
            ...     remove=['existing'], spider='spider2')
            2
        """
        spider = spider or (self.spider.name if self.spider else None)
        if not spider:
            raise ValueError('Please provide spider')
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'project': self.project_id, 'spider': spider})
        result = self._client._connection._post('jobs_update', 'json', params)
        return result['count']


class Job(object):
    """Class representing a job object.

    Not a public constructor: use :class:`~scrapinghub.client.ScrapinghubClient`
    instance or :class:`Jobs` instance to get a :class:`Job` instance. See
    :meth:`scrapinghub.client.ScrapinghubClient.get_job` and :meth:`Jobs.get`
    methods.

    :ivar project_id: integer project id.
    :ivar key: a job key.
    :ivar items: :class:`~scrapinghub.client.items.Items` resource object.
    :ivar logs: :class:`~scrapinghub.client.logs.Logs` resource object.
    :ivar requests: :class:`~scrapinghub.client.requests.Requests` resource object.
    :ivar samples: :class:`~scrapinghub.client.samples.Samples` resource object.
    :ivar metadata: :class:`JobMeta` resource object.

    Usage::

        >>> job = project.jobs.get('123/1/2')
        >>> job.key
        '123/1/2'
        >>> job.metadata.get('state')
        'finished'
    """
    def __init__(self, client, job_key):
        self.project_id = parse_job_key(job_key).project_id
        self.key = job_key

        self._client = client
        self._project = client._hsclient.get_project(self.project_id)
        self._job = client._hsclient.get_job(job_key)

        # proxied sub-resources
        self.items = Items(_Items, client, job_key)
        self.logs = Logs(_Logs, client, job_key)
        self.requests = Requests(_Requests, client, job_key)
        self.samples = Samples(_Samples, client, job_key)

        self.metadata = JobMeta(_JobMeta, client, job_key)

    def update_tags(self, add=None, remove=None):
        """Partially update job tags.

        It provides a convenient way to mark specific jobs (for better search,
        postprocessing etc).

        :param add: (optional) list of tags to add.
        :param remove: (optional) list of tags to remove.

        Usage: to mark a job with tag ``consumed``::

            >>> job.update_tags(add=['consumed'])
        """
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        params.update({'project': self.project_id, 'job': self.key})
        self._client._connection._post('jobs_update', 'json', params)

    def close_writers(self):
        """Stop job batch writers threads gracefully.

        Called on :meth:`ScrapinghubClient.close` method.
        """
        self._job.close_writers()

    def start(self, **params):
        """Move job to running state.

        :param \*\*params: (optional) keyword meta parameters to update.
        :return: a previous string job state.
        :rtype: :class:`str`

        Usage::

            >>> job.start()
            'pending'
        """
        return self.update(state='running', **params)

    def finish(self, **params):
        """Move running job to finished state.

        :param \*\*params: (optional) keyword meta parameters to update.
        :return: a previous string job state.
        :rtype: :class:`str`

        Usage::

            >>> job.finish()
            'running'
        """
        return self.update(state='finished', **params)

    def delete(self, **params):
        """Mark finished job for deletion.

        :param \*\*params: (optional) keyword meta parameters to update.
        :return: a previous string job state.
        :rtype: :class:`str`

        Usage::

            >>> job.delete()
            'finished'
        """
        return self.update(state='deleted', **params)

    def update(self, state, **params):
        """Update job state.

        :param state: a new job state.
        :param \*\*params: (optional) keyword meta parameters to update.
        :return: a previous string job state.
        :rtype: :class:`str`

        Usage::

            >>> job.update('finished')
            'running'
        """
        try:
            job = next(self._project.jobq.update(self, state=state, **params))
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


class JobMeta(_MappingProxy):
    """Class representing job metadata.

    Not a public constructor: use :class:`Job` instance to get a
    :class:`JobMeta` instance. See :attr:`~Job.metadata` attribute.

    Usage:

    - get job metadata instance::

        >>> job.metadata
        <scrapinghub.client.jobs.JobMeta at 0x10494f198>

    - iterate through job metadata::

        >>> job.metadata.iter()
        <dict_itemiterator at 0x104adbd18>

    - list job metadata::

        >>> job.metadata.list()
        [('project', 123), ('units', 1), ('state', 'finished'), ...]

    - get meta field value by name::

        >>> job.metadata.get('version')
        'test'

    - update job meta field value (some meta fields are read-only)::

        >>> job.metadata.set('my-meta', 'test')

    - update multiple meta fields at once

        >>> job.metadata.update({'my-meta1': 'test1', 'my-meta2': 'test2'})

    - delete meta field by name::

        >>> job.metadata.delete('my-meta')
    """
