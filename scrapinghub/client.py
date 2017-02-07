import json

from scrapinghub import APIError
from scrapinghub import Connection as _Connection
from scrapinghub import HubstorageClient as _HubstorageClient

from .hubstorage.resourcetype import DownloadableResource
from .hubstorage.resourcetype import ItemsResourceType

# scrapinghub.hubstorage classes to use as-is
from .hubstorage.frontier import Frontier
from .hubstorage.job import JobMeta
from .hubstorage.project import Settings

# scrapinghub.hubstorage proxied classes
from .hubstorage.activity import Activity as _Activity
from .hubstorage.collectionsrt import Collections as _Collections
from .hubstorage.collectionsrt import Collection as _Collection
from .hubstorage.job import Items as _Items
from .hubstorage.job import Logs as _Logs
from .hubstorage.job import Samples as _Samples
from .hubstorage.job import Requests as _Requests

from .exceptions import (
    NotFound, DuplicateJobError, wrap_http_errors, wrap_value_too_large
)
from .utils import LogLevel
from .utils import get_tags_for_update
from .utils import parse_project_id, parse_job_key
from .utils import proxy_methods
from .utils import wrap_kwargs


class Connection(_Connection):

    @wrap_http_errors
    def _request(self, *args, **kwargs):
        return super(Connection, self)._request(*args, **kwargs)


class HubstorageClient(_HubstorageClient):

    @wrap_http_errors
    def request(self, *args, **kwargs):
        return super(HubstorageClient, self).request(*args, **kwargs)


class ScrapinghubClient(object):
    """Main class to work with Scrapinghub API.

    :param apikey: Scrapinghub APIKEY string.
    :param dash_endpoint: (optional) Scrapinghub Dash panel url.
    :param \*\*kwargs: (optional) Additional arguments for
        :class:`scrapinghub.hubstorage.HubstorageClient` constructor.

    :ivar projects: projects collection, :class:`Projects` instance.

    Usage::

        >>> from scrapinghub import ScrapinghubClient
        >>> client = ScrapinghubClient('APIKEY')
        >>> client
        <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>
    """

    def __init__(self, apikey=None, dash_endpoint=None, **kwargs):
        self.projects = Projects(self)
        self._connection = Connection(apikey=apikey, url=dash_endpoint)
        self._hsclient = HubstorageClient(auth=apikey, **kwargs)

    def get_project(self, projectid):
        """Get :class:`Project` instance with a given project id.

        The method is a shortcut for client.projects.get().

        :param projectid: integer or string numeric project id.
        :return: :class:`Project` object.
        :rtype: scrapinghub.client.Project.

        Usage::

            >>> project = client.get_project(123)
            >>> project
            <scrapinghub.client.Project at 0x106cdd6a0>
        """
        return self.projects.get(parse_project_id(projectid))

    def get_job(self, jobkey):
        """Get Job with a given jobkey.

        :param jobkey: job key string in format 'project/spider/job',
            where all the components are integers.
        :return: :class:`Job` object.
        :rtype: scrapinghub.client.Job.

        Usage::

            >>> job = client.get_job('1/2/3')
            >>> job
            <scrapinghub.client.Job at 0x10afe2eb1>
        """
        projectid = parse_job_key(jobkey).projectid
        return self.projects.get(projectid).jobs.get(jobkey)

    def close(self, timeout=None):
        """Close client instance.

        :param timeout: (optional) float timeout secs to stop everything
            gracefully.
        """
        self._hsclient.close(timeout=timeout)


class Projects(object):
    """Collection of projects available to current user.

    Not a public constructor: use :class:`Scrapinghub` client instance to get
    a :class:`Projects` instance. See :attr:`Scrapinghub.projects` attribute.

    Usage::

        >>> client.projects
        <scrapinghub.client.Projects at 0x1047ada58>
    """

    def __init__(self, client):
        self._client = client

    def get(self, projectid):
        """Get project for a given project id.

        :param projectid: integer or string numeric project id.
        :return: :class:`Project` object.
        :rtype: scrapinghub.client.Project.

        Usage::

            >>> project = client.projects.get(123)
            >>> project
            <scrapinghub.client.Project at 0x106cdd6a0>
        """
        return Project(self._client, parse_project_id(projectid))

    def list(self):
        """Get list of projects available to current user.

        :return: a list of integer project ids.

        Usage::

            >>> client.projects.list()
            [123, 456]
        """
        return self._client._connection.project_ids()

    def summary(self, **params):
        """Get short summaries for all available user projects.

        :return: a list of dictionaries: each dictionary represents a project
            summary (amount of pending/running/finished jobs and a flag if it
            has a capacity to schedule new jobs).

        Usage::

            >>> client.projects.summary()
            [{'finished': 674,
              'has_capacity': True,
              'pending': 0,
              'project': 123,
              'running': 1},
             {'finished': 33079,
              'has_capacity': True,
              'pending': 0,
              'project': 456,
              'running': 2}]
        """
        return self._client._hsclient.projects.jobsummaries(**params)


class Project(object):
    """Class representing a project object and its resources.

    Not a public constructor: use :class:`ScrapinghubClient` instance or
    :class:`Projects` instance to get a :class:`Project` instance. See
    :meth:`Scrapinghub.get_project` or :meth:`Projects.get_project` methods.

    :ivar id: integer project id.
    :ivar activity: :class:`Activity` resource object.
    :ivar collections: :class:`Collections` resource object.
    :ivar frontier: :class:`Frontier` resource object.
    :ivar jobs: :class:`Jobs` resource object.
    :ivar settings: :class:`Settings` resource object.
    :ivar spiders: :class:`Spiders` resource object.

    Usage::

        >>> project = client.get_project(123)
        >>> project
        <scrapinghub.client.Project at 0x106cdd6a0>
    """

    def __init__(self, client, projectid):
        self.key = projectid
        self._client = client

        # sub-resources
        self.jobs = Jobs(client, projectid)
        self.spiders = Spiders(client, projectid)

        # proxied sub-resources
        self.activity = Activity(_Activity, client, projectid)
        self.collections = Collections(_Collections, client, projectid)
        self.frontier = Frontier(client._hsclient, projectid)
        self.settings = Settings(client._hsclient, projectid)


class Spiders(object):
    """Class to work with a collection of project spiders.

    Not a public constructor: use :class:`Project` instance to get
    a :class:`Spiders` instance. See :attr:`Project.spiders` attribute.

    :ivar projectid: integer project id.

    Usage::

        >>> project.spiders
        <scrapinghub.client.Spiders at 0x1049ca630>
    """

    def __init__(self, client, projectid):
        self.projectid = projectid
        self._client = client

    def get(self, spidername, **params):
        """Get a spider object for a given spider name.

        The method gets/sets spider id (and checks if spider exists).

        :param spidername: a string spider name.
        :return: :class:`Spider` object.
        :rtype: scrapinghub.client.Spider.

        Usage::

            >>> project.spiders.get('spider2')
            <scrapinghub.client.Spider at 0x106ee3748>
            >>> project.spiders.get('non-existing')
            NotFound: Spider non-existing doesn't exist.
        """
        project = self._client._hsclient.get_project(self.projectid)
        spiderid = project.ids.spider(spidername, **params)
        if spiderid is None:
            raise NotFound("Spider {} doesn't exist.".format(spidername))
        return Spider(self._client, self.projectid, spiderid, spidername)

    def list(self):
        """Get a list of spiders for a project.

        :return: a list of dictionaries with spiders metadata.

        Usage::  # noqa

            >>> project.spiders.list()
            [{'id': 'spider1', 'tags': [], 'type': 'manual', 'version': '123'},
             {'id': 'spider2', 'tags': [], 'type': 'manual', 'version': '123'}]
        """
        project = self._client._connection[self.projectid]
        return project.spiders()


class Spider(object):
    """Class representing a Spider object.

    Not a public constructor: use :class:`Spiders` instance to get
    a :class:`Spider` instance. See :meth:`Spiders.get` method.

    :ivar projectid: integer project id.
    :ivar id: integer spider id.
    :ivar name: a spider name string.
    :ivar jobs: a collection of jobs, :class:`Jobs` object.

    Usage::

        >>> project.spiders.get('spider2')
        <scrapinghub.client.Spider at 0x106ee3748>
        >>> spider.key
        2
        >>> spider.name
        spider2
    """

    def __init__(self, client, projectid, spiderid, spidername):
        self.projectid = projectid
        self.key = spiderid
        self.name = spidername
        self.jobs = Jobs(client, projectid, self)
        self._client = client


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
        >>> spider = project.spiders.get('spider2')
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
            ...     spider='foo', state='finished', count=3)
        """
        if self.spider:
            params['spider'] = self.spider.name
        return self._project.jobq.list(**params)

    def schedule(self, spidername=None, **params):
        """Schedule a new job and returns its jobkey.

        :param spidername: a spider name string
            (not needed if job is scheduled via :attr:`Spider.jobs`).
        :param \*\*params: (optional) additional keyword args.
        :return: a jobkey string pointing to the new job.

        Usage::

            >>> project.schedule('myspider', arg1='val1')
            '123/1/1'
        """
        if not spidername and not self.spider:
            raise ValueError('Please provide spidername')
        params['project'] = self.projectid
        params['spider'] = spidername or self.spider.name
        if 'job_settings' in params:
            params['job_settings'] = json.dumps(params['job_settings'])
        if 'meta' in params:
            params['meta'] = json.dumps(params['meta'])
        # FIXME improve to schedule multiple jobs
        try:
            response = self._client._connection._post(
                'schedule', 'json', params)
        except APIError as exc:
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
        if self.spider and jobkey.spiderid != self.spider.key:
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
            return self.spider.key
        elif spiderid and self.spider and spiderid != self.spider.key:
            raise ValueError('Please use same spider id')
        return spiderid

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

            >>> spider.jobs.update_tags(add=['consumed'])
            5

        - remove existing tag ``existing`` for all spider jobs::

            >>> project.jobs.update_tags(
            ...     remove=['existing'], spidername='spider')
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
        >>> job.metadata['state']
        'finished'
    """
    def __init__(self, client, jobkey, metadata=None):
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

        self.metadata = JobMeta(client._hsclient, jobkey, cached=metadata)

    def update_metadata(self, *args, **kwargs):
        """Update job metadata.

        :param \*\*kwargs: keyword arguments representing job metadata

        Usage:

        - update job outcome::

            >>> job.update_metadata(close_reason='custom reason')

        - change job tags::

            >>> job.update_metadata({'tags': 'obsolete'})
        """
        self._job.update_metadata(*args, **kwargs)

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
        if not params:
            return
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
            >>> job.metadata['cancelled_by']
            'John'
        """
        self._project.jobq.request_cancel(self)

    def purge(self):
        """Delete job and expire its local metadata.

        Usage::

            >>> job.purge()
            >>> job.metadata['state']
            'deleted'
        """
        self.delete()
        self.metadata.expire()


class _Proxy(object):
    """A helper to create a class instance and proxy its methods to origin.

    The internal proxy class is useful to link class attributes from its
    origin depending on the origin base class as a part of init logic:

    - :class:`ItemsResourceType` provides items-based attributes to access
    items in an arbitrary collection with get/write/flush/close/stats/iter
    methods.

    - :class:`DownloadableResource` provides download-based attributes to
    iter through collection with or without msgpack support.
    """

    def __init__(self, cls, client, key):
        self.key = key
        self._client = client
        self._origin = cls(client._hsclient, key)

        if issubclass(cls, ItemsResourceType):
            self._proxy_methods(['get', 'write', 'flush', 'close',
                                 'stats', ('iter', 'list')])
            # redefine write method to wrap hubstorage.ValueTooLarge error
            origin_method = getattr(self, 'write')
            setattr(self, 'write', wrap_value_too_large(origin_method))

        # DType iter_values() has more priority than IType list()
        if issubclass(cls, DownloadableResource):
            methods = [('iter', 'iter_values'),
                       ('iter_raw_msgpack', 'iter_msgpack'),
                       ('iter_raw_json', 'iter_json')]
            self._proxy_methods(methods)

            # apply_iter_filters is responsible to modify filter params for all
            # iter* calls: should be used only if defined for a child class
            if hasattr(self, '_modify_iter_filters'):
                apply_fn = getattr(self, '_modify_iter_filters')
                for method in [method[0] for method in methods]:
                    wrapped = wrap_kwargs(getattr(self, method), apply_fn)
                    setattr(self, method, wrapped)

    def _proxy_methods(self, methods):
        """A little helper for cleaner interface."""
        proxy_methods(self._origin, self, methods)


class Logs(_Proxy):
    """Representation of collection of job logs.

    Not a public constructor: use :class:`Job` instance to get a :class:`Logs`
    instance. See :attr:`Job.logs` attribute.

    Usage:

    - retrieve all logs from a job::

        >>> job.logs.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - retrieve a single log entry from a job::

        >>> list(job.logs.iter(count=1))
        [{
            'level': 20,
            'message': '[scrapy.core.engine] Closing spider (finished)',
            'time': 1482233733976},
        }]
    """

    def __init__(self, *args, **kwargs):
        super(Logs, self).__init__(*args, **kwargs)
        self._proxy_methods(['log', 'debug', 'info', 'warning', 'warn',
                             'error', 'batch_write_start'])

    def _modify_iter_filters(self, params):
        """Modify iter() filters on-the-fly.

        - convert offset to start parameter
        - check log level and create a corresponding meta filter

        :param params: an original dictionary with params
        :return: a modified dictionary with params
        """
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '{}/{}'.format(self.key, offset)
        level = params.pop('level', None)
        if level:
            minlevel = getattr(LogLevel, level, None)
            if minlevel is None:
                raise ValueError("Unknown log level: {}".format(level))
            params['filter'] = json.dumps(['level', '>=', [minlevel]])
        return params


class Items(_Proxy):
    """Representation of collection of job items.

    Not a public constructor: use :class:`Job` instance to get a :class:`Items`
    instance. See :attr:`Job.items` attribute.

    Usage:

    - retrieve all scraped items from a job::

        >>> job.items.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - retrieve items with timestamp greater or equal to given timestamp
      (item here is an arbitrary dictionary depending on your code)::

        >>> list(job.items.iter(startts=1447221694537))
        {'name': ['Some custom item'],
         'url': 'http://some-url/item.html'}]
    """

    def _modify_iter_filters(self, params):
        """Modify iter filter to convert offset to start parameter.

        Returns:
            dict: updated set of params
        """
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '{}/{}'.format(self.key, offset)
        return params


class Requests(_Proxy):
    """Representation of collection of job requests.

    Not a public constructor: use :class:`Job` instance to get a
    :class:`Requests` instance. See :attr:`Job.requests` attribute.

    Usage:

    - retrieve all requests from a job::

        >>> job.requests.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - iterate through the requests::

        >>> for reqitem in job.requests.iter(count=1):
        ...     print(reqitem['time'])
        1482233733870

    - retrieve single request from a job::

        >>> list(job.requests.iter(count=1))
        [{
        'duration': 354,
        'fp': '6d748741a927b10454c83ac285b002cd239964ea',
        'method': 'GET',
        'rs': 1270,
        'status': 200,a
        'time': 1482233733870,
        'url': 'https://example.com'
        }]
    """
    def __init__(self, *args, **kwargs):
        super(Requests, self).__init__(*args, **kwargs)
        self._proxy_methods(['add'])


class Samples(_Proxy):
    """Representation of collection of job samples.

    Not a public constructor: use :class:`Job` instance to get a
    :class:`Samples` instance. See :attr:`Job.samples` attribute.

    Usage:

    - retrieve all samples from a job::

        >>> job.samples.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - retrieve samples with timestamp greater or equal to given timestamp::

        >>> list(job.samples.iter(startts=1484570043851))
        [[1484570043851, 554, 576, 1777, 821, 0],
         [1484570046673, 561, 583, 1782, 821, 0]]
    """


class Activity(_Proxy):
    """Representation of collection of job activity events.

    Not a public constructor: use :class:`Project` instance to get a
    :class:`Activity` instance. See :attr:`Project.activity` attribute.

    Usage:

    - get all activity from a project::

        >>> project.activity.iter()
        <generator object jldecode at 0x1049ee990>

    - get only last 2 events from a project::

        >>> list(p.activity.iter(count=2))
        [{'event': 'job:completed', 'job': '123/2/3', 'user': 'jobrunner'},
         {'event': 'job:cancelled', 'job': '123/2/3', 'user': 'john'}]
    """
    def __init__(self, *args, **kwargs):
        super(Activity, self).__init__(*args, **kwargs)
        self._proxy_methods([('iter', 'list')])

    def add(self, *args, **kwargs):
        self._origin.add(*args, **kwargs)

    def post(self, *args, **kwargs):
        self._origin.post(*args, **kwargs)


class Collections(_Proxy):
    """Access to project collections.

    Not a public constructor: use :class:`Project` instance to get a
    :class:`Collections` instance. See :attr:`Project.collections` attribute.

    Usage::

        >>> collections = project.collections
        >>> foo_store = collections.get_store('foo_store')
    """

    def get(self, coltype, colname):
        """Base method to get a collection with a given type and name."""
        self._origin._validate_collection(coltype, colname)
        return Collection(self._client, self, coltype, colname)

    def get_store(self, colname):
        return self.get('s', colname)

    def get_cached_store(self, colname):
        return self.get('cs', colname)

    def get_versioned_store(self, colname):
        return self.get('vs', colname)

    def get_versioned_cached_store(self, colname):
        return self.get('vcs', colname)


class Collection(object):
    """Representation of a project collection object.

    Not a public constructor: use :class:`Collections` instance to get a
    :class:`Collection` instance. See :meth:`Collections.get_store` and
    similar methods.  # noqa

    Usage:

    - add a new item to collection::

        >>> foo_store.set({'_key': '002d050ee3ff6192dcbecc4e4b4457d7',
                           'value': '1447221694537'})

    - count items in collection::

        >>> foo_store.count()
        1

    - get an item from collection::

        >>> foo_store.get('002d050ee3ff6192dcbecc4e4b4457d7')
        {'value': '1447221694537'}

    - get all items from collection::

        >>> foo_store.iter()
        <generator object jldecode at 0x1049eef10>

    - iterate iterate over _key & value pair::

        >>> list(foo_store.iter())
            [{'_key': '002d050ee3ff6192dcbecc4e4b4457d7',
              'value': '1447221694537'}]

    - filter by multiple keys, only values for keys that exist will be returned::

        >>> list(foo_store.iter(key=['002d050ee3ff6192dcbecc4e4b4457d7', 'blah']))
        [{'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'}]

    - delete an item by key::

        >>> foo_store.delete('002d050ee3ff6192dcbecc4e4b4457d7')
    """

    def __init__(self, client, collections, coltype, colname):
        self._client = client
        self._origin = _Collection(coltype, colname, collections._origin)
        proxy_methods(self._origin, self, [
            'create_writer', 'get', 'set', 'delete', 'count',
            ('iter', 'iter_values'),
            ('iter_raw_json', 'iter_json'),
        ])

    def get(self, key, *args, **kwargs):
        """Get item from collection by key.

        :param key: string item key
        :return: an item dictionary if exists
        """
        if key is None:
            raise ValueError("key cannot be None")
        return self._origin.get(key, *args, **kwargs)
