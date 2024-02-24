Overview
========

:class:`~scrapinghub.client.ScrapinghubClient` is a Python client for
communicating with the `Scrapinghub API`_.

First, you instantiate a new client with your Scrapinghub API key::

    >>> from scrapinghub import ScrapinghubClient
    >>> apikey = '84c87545607a4bc0****************'
    >>> client = ScrapinghubClient(apikey)
    >>> client
    <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>

Working with projects
---------------------

This client instance has a :attr:`~scrapinghub.client.ScrapinghubClient.projects`
attribute for accessing your projects on Scrapinghub's platform.

With it, you can list the project IDs available in your account::

    >>> client.projects.list()
    [123, 456]

.. note::
    ``.list()`` does not return :class:`~scrapinghub.client.projects.Project`
    instances, but their numeric IDs.

Or you can get a summary of all your projects (how many jobs are finished,
running or pending to be run)::

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


To work with a particular project, reference it using its numeric ID::

    >>> project = client.get_project(123)
    >>> project
    <scrapinghub.client.Project at 0x106cdd6a0>
    >>> project.key
    '123'

.. note::
    ``get_project()`` returns a :class:`~scrapinghub.client.projects.Project`
    instance.

.. tip:: The above is a shortcut for ``client.projects.get(123)``.


Working with spiders
--------------------

A Scrapinghub project (usually) consists of a group of web crawlers
called "spiders".

The different spiders within your project are accessible via the
:class:`spiders <~scrapinghub.client.spiders.Spiders>` attribute of the
:class:`~scrapinghub.client.projects.Project` instance.

To get the list of spiders in the project, use ``.spiders.list()``::

    >>> project.spiders.list()
    [
      {'id': 'spider1', 'tags': [], 'type': 'manual', 'version': '123'},
      {'id': 'spider2', 'tags': [], 'type': 'manual', 'version': '123'}
    ]

.. _spider:

To select a particular spider to work with, use ``.spiders.get(<spidername>)``::

    >>> spider = project.spiders.get('spider2')
    >>> spider
    <scrapinghub.client.Spider at 0x106ee3748>
    >>> spider.key
    '123/2'
    >>> spider.name
    spider2

With ``.spiders.get(<spidername>)``, you get a :class:`~scrapinghub.client.spiders.Spider`
instance back.

.. note::
    ``.spiders.list()`` does not return :class:`~scrapinghub.client.spiders.Spider`
    instances. The ``id`` key in the returned dicts corresponds to
    the ``.name`` attribute of :class:`~scrapinghub.client.spiders.Spider`
    that you get with ``.spiders.get(<spidername>)``.


.. _jobs:

Working with jobs collections
-----------------------------

Essentially, the purpose of spiders is to be run in Scrapinghub's platform.
Each spider run is called a "job".
And a collection of spider jobs is represented by a :class:`~scrapinghub.client.jobs.Jobs`
object.

Both project-level jobs (i.e. all jobs from a project) and spider-level jobs
(i.e. all jobs for a specific spider) are available as a :class:`jobs
<~scrapinghub.client.jobs.Jobs>` attribute of a
:class:`~scrapinghub.client.projects.Project` instance
or a :class:`~scrapinghub.client.spiders.Spider` instance respectively.

Running jobs
^^^^^^^^^^^^

Use the ``.jobs.run()`` method to run a new job for a project or a particular spider,::

    >>> job = spider.jobs.run()

You can also use ``.jobs.run()`` at the project level, the difference being that
a spider name is required::

    >>> job = project.jobs.run('spider1')

Scheduling jobs supports different options, passed as arguments to ``.run()``:

- **job_args** (dict): to provide arguments for the job
- **job_settings** (dict): to pass additional settings for the job
- **units** (integer): to specify amount of units to run the job
- **priority** (integer): to set higher/lower priority for the job
- **add_tag** (list of strings): to create a job with a set of initial tags
- **meta** (dict): to pass additional custom metadata

Check the `run endpoint`_ for more information.

For example, to run a new job for a given spider with custom parameters::

    >>> job = spider.jobs.run(units=2, job_settings={'SETTING': 'VALUE'}, priority=1,
    ...                       add_tag=['tagA','tagB'], meta={'custom-data': 'val1'})



Getting job information
^^^^^^^^^^^^^^^^^^^^^^^

To select a specific job for a project, use ``.jobs.get(<jobKey>)``::

    >>> job = project.jobs.get('123/1/2')
    >>> job.key
    '123/1/2'

Also there's a shortcut to get same job with client instance::

    >>> job = client.get_job('123/1/2')

These methods return a :class:`~scrapinghub.client.jobs.Job` instance
(see :ref:`below <job>`).

Counting jobs
^^^^^^^^^^^^^

It's also possible to count jobs for a given project or spider via
``.jobs.count()``::

    >>> spider.jobs.count()
    5

The counting logic supports different filters, as described for `count endpoint`_.


Iterating over jobs
^^^^^^^^^^^^^^^^^^^

To loop over the spider jobs (most recently finished first),
you can use ``.jobs.iter()`` to get an iterator object::

    >>> jobs_summary = spider.jobs.iter()
    >>> [j['key'] for j in jobs_summary]
    ['123/1/3', '123/1/2', '123/1/1']

The ``.jobs.iter()`` iterator generates dicts
(not :class:`~scrapinghub.client.jobs.Job` objects), e.g::

    {u'close_reason': u'finished',
     u'elapsed': 201815620,
     u'finished_time': 1492843577852,
     u'items': 2,
     u'key': u'123320/3/155',
     u'logs': 21,
     u'pages': 2,
     u'pending_time': 1492843520319,
     u'running_time': 1492843526622,
     u'spider': u'spider001',
     u'state': u'finished',
     u'ts': 1492843563720,
     u'version': u'792458b-master'}

You typically use it like this::

    >>> for job in jobs_summary:
    ...     # do something with job data

Or, if you just want to get the job IDs::

    >>> [x['key'] for x in jobs_summary]
    ['123/1/3', '123/1/2', '123/1/1']

The job's dict fieldset from ``.jobs.iter()`` is less detailed than ``job.metadata`` (see below),
but can contain a few additional fields as well, on demand.
Additional fields can be requested using the ``jobmeta`` argument.

When ``jobmeta`` is used, the user MUST list all required fields,
even default ones::

    >>> # by default, the "spider" key is available in the dict from iter()
    >>> job_summary = next(project.jobs.iter())
    >>> job_summary.get('spider', 'missing')
    'foo'
    >>>
    >>> # when jobmeta is use, if "spider" key is not listed in it,
    >>> # iter() will not include "spider" key in the returned dicts
    >>> jobs_summary = project.jobs.iter(jobmeta=['scheduled_by'])
    >>> job_summary = next(jobs_summary)
    >>> job_summary.get('scheduled_by', 'missing')
    'John'
    >>> job_summary.get('spider', 'missing')
    missing

By default ``.jobs.iter()`` returns the last 1000 jobs at most.
To get more than the last 1000, you need to paginate through results
in batches, using the ``start`` parameter::

    >>> jobs_summary = spider.jobs.iter(start=1000)

There are several filters like ``spider``, ``state``, ``has_tag``,
``lacks_tag``, ``startts`` and ``endts`` (check `list endpoint`_ for more details).

To get jobs filtered by tags::

    >>> jobs_summary = project.jobs.iter(has_tag=['new', 'verified'], lacks_tag='obsolete')

.. warning::
    The list of tags in ``has_tag`` is an *OR* condition, so in the case above,
    jobs with either ``'new'`` or ``'verified'`` tag are selected.

    On the contrary the list of tags in ``lacks_tag`` is a logical *AND*.

To get a specific number of last finished jobs of some spider,
use ``spider``, ``state`` and ``count`` arguments::

    >>> jobs_summary = project.jobs.iter(spider='foo', state='finished', count=3)

There are 4 possible job states, which can be used as (string) values
for filtering by state:

- ``'pending'``: the job is scheduled to run when enough units become available;
- ``'running'``: the job is running;
- ``'finished'``: the job has ended;
- ``'deleted'``: the jobs has been deleted and will become unavailable
  when the platform performs its next cleanup.

Dictionary entries returned by ``.jobs.iter()`` method contain some additional meta,
but can be easily converted to :class:`~scrapinghub.client.jobs.Job` instances with::

    >>> [Job(client, x['key']) for x in jobs]
    [
      <scrapinghub.client.Job at 0x106e2cc18>,
      <scrapinghub.client.Job at 0x106e260b8>,
      <scrapinghub.client.Job at 0x106e26a20>,
    ]

Jobs summaries
^^^^^^^^^^^^^^

To check jobs summary::

    >>> spider.jobs.summary()
    [{'count': 0, 'name': 'pending', 'summary': []},
     {'count': 0, 'name': 'running', 'summary': []},
     {'count': 5,
      'name': 'finished',
      'summary': [...]}

It's also possible to get last jobs summary (for each spider)::

    >>> list(sp.jobs.iter_last())
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

Note that there can be a lot of spiders, so the method above returns an iterator.


Updating tags
^^^^^^^^^^^^^

Tags is a convenient way to mark specific jobs (for better search, postprocessing etc).


To mark all spider jobs with tag ``consumed``::

    >>> spider.jobs.update_tags(add=['consumed'])

To remove existing tag ``existing`` for all spider jobs::

    >>> spider.jobs.update_tags(remove=['existing'])

Modifying tags is available at :class:`~scrapinghub.client.spiders.Spider`
level and :class:`~scrapinghub.client.jobs.Job` level.


Canceling jobs
^^^^^^^^^^^^^^

To cancel a few jobs by keys at once::

    >>> spider.jobs.cancel(['123/1/2', '123/1/3'])

All jobs should belong to the same project.

Note that there's a limit on amount of job keys you can cancel with a single call,
please contact support if the amount is more than 1k.


.. _job:


.. _job-actions:

Job actions
-----------

You can perform actions on a :class:`~scrapinghub.client.jobs.Job` instance.

For example, to cancel a running or pending job, simply call ``cancel()``
on it::

    >>> job.cancel()

To delete a job, its metadata, logs and items, call ``delete()``::

    >>> job.delete()

To mark a job with the tag ``'consumed'``, call ``update_tags()``::

    >>> job.update_tags(add=['consumed'])


.. _job-data:

Job data
--------

A :class:`~scrapinghub.client.jobs.Job` instance provides access to its
associated data, using the following attributes:

- ``metadata``: various information on the job itself;
- ``items``: the data items that the job produced;
- ``logs``: log entries that the job produced;
- ``requests``: HTTP requests that the job issued;
- ``samples``: runtime stats that the job uploaded;


.. _job-metadata:

Metadata
^^^^^^^^

Metadata about a job details can be accessed via its ``metadata`` attribute.
The :class:`corresponding object <scrapinghub.client.jobs.JobMeta>`
acts like a Python dictionary::

    >>> job.metadata.get('version')
    '5123a86-master'

To check what keys are available (they ultimately depend on the job),
you can use its ``.iter()`` method (here, it's wrapped inside a dict for readability)::

    >>> dict(job.metadata.iter())
    {...
     u'close_reason': u'finished',
     u'completed_by': u'jobrunner',
     u'deploy_id': 16,
     u'finished_time': 1493007370566,
     u'job_settings': {u'CLOSESPIDER_PAGECOUNT': 5,
                       u'SOME_CUSTOM_SETTING': 10},
     u'pending_time': 1493006433100,
     u'priority': 2,
     u'project': 123456,
     u'running_time': 1493006488829,
     u'scheduled_by': u'periodicjobs',
     u'scrapystats': {u'downloader/request_bytes': 96774,
                      u'downloader/request_count': 228,
                      u'downloader/request_method_count/GET': 228,
                      u'downloader/response_bytes': 923251,
                      u'downloader/response_count': 228,
                      u'downloader/response_status_count/200': 228,
                      u'finish_reason': u'finished',
                      u'finish_time': 1493007337660.0,
                      u'httpcache/firsthand': 228,
                      u'httpcache/miss': 228,
                      u'httpcache/store': 228,
                      u'item_scraped_count': 684,
                      u'log_count/INFO': 22,
                      u'memusage/max': 63311872,
                      u'memusage/startup': 60248064,
                      u'request_depth_max': 50,
                      u'response_received_count': 228,
                      u'scheduler/dequeued': 228,
                      u'scheduler/dequeued/disk': 228,
                      u'scheduler/enqueued': 228,
                      u'scheduler/enqueued/disk': 228,
                      u'start_time': 1493006508701.0},
     u'spider': u'myspider',
     u'spider_args': {u'arg1': u'value1',
                      u'arg2': u'value2'},
     u'spider_type': u'manual',
     u'started_by': u'jobrunner',
     u'state': u'finished',
     u'tags': [],
     u'units': 1,
     u'version': u'792458b-master'}


As you may have noticed in the example above, if the job was a Scrapy
spider run, the metadata object contains a special ``'scrapystats'`` key,
which is a dict representation of the crawl's `Scrapy stats`_
values::

    >>> job.metadata.get('scrapystats')
    ...
    'downloader/response_count': 104,
    'downloader/response_status_count/200': 104,
    'finish_reason': 'finished',
    'finish_time': 1447160494937,
    'item_scraped_count': 50,
    'log_count/DEBUG': 157,
    'log_count/INFO': 1365,
    'log_count/WARNING': 3,
    'memusage/max': 182988800,
    'memusage/startup': 62439424,
    ...


Anything can be stored in a job's metadata, here is example how to add tags::

    >>> job.metadata.set('tags', ['obsolete'])


.. _Scrapy stats: https://docs.scrapy.org/en/latest/topics/stats.html

.. _job-items:

Items
^^^^^

To retrieve all scraped items (as Python dicts) from a job, use
:class:`job.items.iter() <scrapinghub.client.items.Items>`::

    >>> for item in job.items.iter():
    ...     # do something with item (it's just a dict)

.. _job-logs:

Logs
^^^^

To retrieve all log entries from a job use :class:`job.logs.iter()
<scrapinghub.client.logs.Logs>`::

    >>> for logitem in job.logs.iter():
    ...     # logitem is a dict with level, message, time
    >>> logitem
    {
      'level': 20,
      'message': '[scrapy.core.engine] Closing spider (finished)',
      'time': 1482233733976},
    }

.. _job-requests:

Requests
^^^^^^^^

To retrieve all requests from a job, there's :class:`job.requests.iter()
<scrapinghub.client.requests.Requests>`::

    >>> for reqitem in job.requests.iter():
    ...     # reqitem is a dict
    >>> reqitem
    [{
      'duration': 354,
      'fp': '6d748741a927b10454c83ac285b002cd239964ea',
      'method': 'GET',
      'rs': 1270,
      'status': 200,
      'time': 1482233733870,
      'url': 'https://example.com'
    }]


Project activity log
--------------------

:class:`Project.activity <scrapinghub.client.activity.Activity>` provides a
convenient interface to project activity events.

To retrieve activity events from a project, you can use ``.activity.iter()``,
with optional arguments (here, the last 3 events, with timestamp information)::

    >>> list(project.activity.iter(count=3, meta="_ts"))
    [{u'_ts': 1493362000130,
      u'event': u'job:completed',
      u'job': u'123456/3/161',
      u'user': u'jobrunner'},
     {u'_ts': 1493361946077,
      u'event': u'job:started',
      u'job': u'123456/3/161',
      u'user': u'jobrunner'},
     {u'_ts': 1493361942440,
      u'event': u'job:scheduled',
      u'job': u'123456/3/161',
      u'user': u'periodicjobs'}]

To retrieve all the events, use ``.activity.list()``

    >>> project.activity.list()
    [{'event': 'job:completed', 'job': '123/2/3', 'user': 'jobrunner'},
     {'event': 'job:cancelled', 'job': '123/2/3', 'user': 'john'}]

To post a new activity event, use ``.activity.add()``::

    >>> event = {'event': 'job:completed', 'job': '123/2/4', 'user': 'john'}
    >>> project.activity.add(event)

Or post multiple events at once::

    >>> events = [
    ...     {'event': 'job:completed', 'job': '123/2/5', 'user': 'john'},
    ...     {'event': 'job:cancelled', 'job': '123/2/6', 'user': 'john'},
    ... ]
    >>> project.activity.add(events)


Collections
-----------

Scrapinghub’s Collections provide a way to store an arbitrary number of
records indexed by a key.
They’re often used by Scrapinghub projects as a single place to write
information from multiple scraping jobs.

Read more about *Collections* `in the official docs`_.

As an example, let's store a hash and timestamp pair for spider 'foo'.

The usual workflow with :class:`project.collections
<scrapinghub.client.collections.Collections>` would be:

1. reference your project's ``collections`` attribute,
2. call ``.get_store(<somename>)`` to create or access the named collection
   you want (the collection will be created automatically if it doesn't exist) ;
   you get a "store" object back,
3. call ``.set(<key/value> pairs)`` to store data.

::

    >>> collections = project.collections
    >>> foo_store = collections.get_store('foo_store')
    >>> foo_store.set({'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'})
    >>> foo_store.count()
    1
    >>> foo_store.get('002d050ee3ff6192dcbecc4e4b4457d7')
    {u'value': u'1447221694537'}
    >>> # iterate over _key & value pair
    ... list(foo_store.iter())
    [{u'_key': u'002d050ee3ff6192dcbecc4e4b4457d7', u'value': u'1447221694537'}]
    >>> # filter by multiple keys - only values for keys that exist will be returned
    ... list(foo_store.iter(key=['002d050ee3ff6192dcbecc4e4b4457d7', 'blah']))
    [{u'_key': u'002d050ee3ff6192dcbecc4e4b4457d7', u'value': u'1447221694537'}]
    >>> foo_store.delete('002d050ee3ff6192dcbecc4e4b4457d7')
    >>> foo_store.count()
    0

Collections are available at project level only.

.. _in the official docs: https://doc.scrapinghub.com/api/collections.html


Frontiers
---------

Typical workflow with :class:`~scrapinghub.client.frontiers.Frontiers`::

    >>> frontiers = project.frontiers

Get all frontiers from a project to iterate through it::

    >>> frontiers.iter()
    <list_iterator at 0x103c93630>

List all frontiers::

    >>> frontiers.list()
    ['test', 'test1', 'test2']

Get a :class:`~scrapinghub.client.frontiers.Frontier` instance by name::

    >>> frontier = frontiers.get('test')
    >>> frontier
    <scrapinghub.client.Frontier at 0x1048ae4a8>

Get an iterator to iterate through a frontier slots::

    >>> frontier.iter()
    <list_iterator at 0x1030736d8>

List all slots::

    >>> frontier.list()
    ['example.com', 'example.com2']

Get a :class:`~scrapinghub.client.frontiers.FrontierSlot` by name::

    >>> slot = frontier.get('example.com')
    >>> slot
    <scrapinghub.client.FrontierSlot at 0x1049d8978>

Add a request to the slot::

    >>> slot.queue.add([{'fp': '/some/path.html'}])
    >>> slot.flush()
    >>> slot.newcount
    1

``newcount`` is defined per slot, but also available per frontier and globally::

    >>> frontier.newcount
    1
    >>> frontiers.newcount
    3

Add a fingerprint only to the slot::

    >>> slot.fingerprints.add(['fp1', 'fp2'])
    >>> slot.flush()

There are convenient shortcuts: ``f`` for ``fingerprints`` to access
:class:`~scrapinghub.client.frontiers.FrontierSlotFingerprints` and ``q`` for
``queue`` to access :class:`~scrapinghub.client.frontiers.FrontierSlotQueue`.

Add requests with additional parameters::

    >>> slot.q.add([{'fp': '/'}, {'fp': 'page1.html', 'p': 1, 'qdata': {'depth': 1}}])
    >>> slot.flush()

To retrieve all requests for a given slot::

    >>> reqs = slot.q.iter()

To retrieve all fingerprints for a given slot::

    >>> fps = slot.f.iter()

To list all the requests use ``list()`` method (similar for ``fingerprints``)::

    >>> fps = slot.q.list()

To delete a batch of requests::

    >>> slot.q.delete('00013967d8af7b0001')

To delete the whole slot from the frontier::

    >>> slot.delete()

Flush data of the given frontier::

    >>> frontier.flush()

Flush data of all frontiers of a project::

    >>> frontiers.flush()

Close batch writers of all frontiers of a project::

    >>> frontiers.close()

Frontiers are available on project level only.

.. _job-tags:


Settings
--------

You can work with project settings via :class:`~scrapinghub.client.projects.Settings`.

To get a list of the project settings::

    >>> project.settings.list()
    [(u'default_job_units', 2), (u'job_runtime_limit', 24)]]

To get a project setting value by name::

    >>> project.settings.get('job_runtime_limit')
    24

To update a project setting value by name::

    >>> project.settings.set('job_runtime_limit', 20)

Or update a few project settings at once::

    >>> project.settings.update({'default_job_units': 1,
    ...                          'job_runtime_limit': 20})


Exceptions
----------

.. autoexception:: scrapinghub.ScrapinghubAPIError
.. autoexception:: scrapinghub.BadRequest
.. autoexception:: scrapinghub.Unauthorized
.. autoexception:: scrapinghub.NotFound
.. autoexception:: scrapinghub.ValueTooLarge
.. autoexception:: scrapinghub.DuplicateJobError
.. autoexception:: scrapinghub.ServerError


.. _Scrapinghub API: https://doc.scrapinghub.com/scrapy-cloud.html#scrapycloud
.. _Frontier: https://doc.scrapinghub.com/api/frontier.html
.. _count endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-count
.. _list endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-list
.. _run endpoint: https://doc.scrapinghub.com/api/jobs.html#run-json
