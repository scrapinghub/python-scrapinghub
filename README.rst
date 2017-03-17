====================================
Client interface for Scrapinghub API
====================================

.. image:: https://secure.travis-ci.org/scrapinghub/python-scrapinghub.png?branch=master
   :target: http://travis-ci.org/scrapinghub/python-scrapinghub


The ``scrapinghub`` is a Python library for communicating with the `Scrapinghub API`_.


.. contents:: :depth: 2


Requirements
============

* Python 2.7 or above


Installation
============

The quick way::

    pip install scrapinghub

You can also install the library with MessagePack support, it provides better
response time and improved bandwidth usage::

    pip install scrapinghub[msgpack]


New client
==========

The ``scrapinghub.ScrapinghubClient`` is a new Python client for communicating
with the `Scrapinghub API`_. It takes best from ``scrapinghub.Connection`` and
``scrapinghub.HubstorageClient`` and combines it under single interface.

First, you instantiate new client::

    >>> from scrapinghub import ScrapinghubClient
    >>> client = ScrapinghubClient('APIKEY')
    >>> client
    <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>

Client instance has ``projects`` field for access to client projects.

Projects
--------

You can list the projects available to your account::

    >>> client.projects.list()
    [123, 456]

Or check the projects summary::

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

And select a particular project to work with::

    >>> project = client.get_project(123)
    >>> project
    <scrapinghub.client.Project at 0x106cdd6a0>
    >>> project.key
    '123'

The above is a shortcut for ``client.projects.get(123)``.

Project
-------

Project instance has ``jobs`` field to work with the project jobs.

Jobs instance is described well in ``Jobs`` section below.

For example, to schedule a spider run (it returns a job object)::

    >>> project.jobs.schedule('spider1', job_args={'arg1':'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>

Project instance also has the following fields:

- activity - access to project activity records
- collections - work with project collections (see ``Collections`` section)
- frontiers - using project frontier (see ``Frontiers`` section)
- settings - interface to project settings
- spiders - access to spiders collection (see ``Spiders`` section)


Settings
--------

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


Spiders
-------

To get the list of spiders of the project::

    >>> project.spiders.list()
    [
      {'id': 'spider1', 'tags': [], 'type': 'manual', 'version': '123'},
      {'id': 'spider2', 'tags': [], 'type': 'manual', 'version': '123'}
    ]

To select a particular spider to work with::

    >>> spider = project.spiders.get('spider2')
    >>> spider
    <scrapinghub.client.Spider at 0x106ee3748>
    >>> spider.key
    '123/2'
    >>> spider.name
    spider2

Spider
------

Like project instance, spider instance has ``jobs`` field to work with the spider's jobs.

To schedule a spider run::

    >>> spider.jobs.schedule(job_args={'arg1:'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>

Note that you don't need to specify spider name explicitly.

Jobs
----

Jobs collection is available on project/spider level.

get
^^^

To select a specific job for a project::

    >>> job = project.jobs.get('123/1/2')
    >>> job.key
    '123/1/2'

Also there's a shortcut to get same job with client instance::

    >>> job = client.get_job('123/1/2')

schedule
^^^^^^^^

Use ``schedule`` method to schedule a new job for project/spider::

    >>> job = spider.jobs.schedule()

Scheduling logic supports different options, like

- spider_args to provide spider arguments for the job
- units to specify amount of units to schedule the job
- job_settings to pass additional settings for the job
- priority to set higher/lower priority of the job
- add_tag to create a job with a set of initial tags
- meta to pass additional custom metadata

For example, to schedule a new job for a given spider with custom params::

    >>> job = spider.jobs.schedule(units=2, job_settings={'SETTING': 'VALUE'},
        priority=1, add_tag=['tagA','tagB'], meta={'custom-data': 'val1'})

Note that if you schedule a job on project level, spider name is required::

    >>> job = project.jobs.schedule('spider1')

count
^^^^^

It's also possible to count jobs for a given project/spider::

    >>> spider.jobs.count()
    5

Count logic supports different filters, as described for `count endpoint`_.


iter
^^^^

To iterate through the spider jobs (descending order)::

    >>> jobs_summary = spider.jobs.iter()
    >>> [j['key'] for j in jobs_summary]
    ['123/1/3', '123/1/2', '123/1/1']

``jobs_summary`` is an iterator and, when iterated, returns an iterable
of dict objects, so you typically use it like this::

    >>> for job in jobs_summary:
    ...     # do something with job data

Or, if you just want to get the job ids::

    >>> [x['key'] for x in jobs_summary]
    ['123/1/3', '123/1/2', '123/1/1']

Job summary fieldset from ``iter()`` is less detailed than ``job.metadata``,
but contains few new fields as well. Additional fields can be requested using
the ``jobmeta`` parameter. If it used, then it's up to the user to list all the
required fields, so only few default fields would be added except requested
ones::

    >>> job_summary = next(project.jobs.iter())
    >>> job_summary.get('spider', 'missing')
    'foo'
    >>> jobs_summary = project.jobs.iter(jobmeta=['scheduled_by', ])
    >>> job_summary = next(jobs_summary)
    >>> job_summary.get('scheduled_by', 'missing')
    'John'
    >>> job_summary.get('spider', 'missing')
    missing

By default ``jobs.iter()`` returns maximum last 1000 results.
Pagination is available using the ``start`` parameter::

    >>> jobs_summary = spider.jobs.iter(start=1000)

There are several filters like spider, state, has_tag, lacks_tag,
startts and endts (check `list endpoint`_ for more details).

To get jobs filtered by tags::

    >>> jobs_summary = project.jobs.iter(has_tag=['new', 'verified'], lacks_tag='obsolete')

List of tags has ``OR`` power, so in the case above jobs with 'new' or
'verified' tag are expected.

To get certain number of last finished jobs per some spider::

    >>> jobs_summary = project.jobs.iter(spider='foo', state='finished', count=3)

There are 4 possible job states, which can be used as values
for filtering by state:

- pending
- running
- finished
- deleted

Dict entries returned by ``iter`` method contain some additional meta,
but can be easily converted to ``Job`` instances with::

    >>> [Job(x['key']) for x in jobs]
    [
      <scrapinghub.client.Job at 0x106e2cc18>,
      <scrapinghub.client.Job at 0x106e260b8>,
      <scrapinghub.client.Job at 0x106e26a20>,
    ]

summary
^^^^^^^

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

Job
---

Job instance provides access to a job data with the following fields:

- metadata
- items
- logs
- requests
- samples

Request to cancel a job::

    >>> job.cancel()

To delete a job::

    >>> job.delete()

Metadata
^^^^^^^^

Job details can be found in jobs metadata and it's scrapystats::

    >>> job.metadata.get('version')
    '5123a86-master'
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

Anything can be stored in metadata, here is example how to add tags::

    >>> job.metadata.set('tags', ['obsolete'])

Items
^^^^^

To retrieve all scraped items from a job::

    >>> for item in job.items.iter():
    ...     # do something with item (it's just a dict)

Logs
^^^^

To retrieve all log entries from a job::

    >>> for logitem in job.logs.iter():
    ...     # logitem is a dict with level, message, time
    >>> logitem
    {
      'level': 20,
      'message': '[scrapy.core.engine] Closing spider (finished)',
      'time': 1482233733976},
    }

Requests
^^^^^^^^

To retrieve all requests from a job::

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

Samples
^^^^^^^

To retrieve all samples for a job::

    >>> for sample in job.samples.iter():
    ...     # sample is a list with a timestamp and data
    >>> sample
    [1482233732452, 0, 0, 0, 0, 0]


Activity
--------

To retrieve all activity events from a project::

    >>> project.activity.iter()
    <generator object jldecode at 0x1049ee990>

    >>> project.activity.list()
    [{'event': 'job:completed', 'job': '123/2/3', 'user': 'jobrunner'},
     {'event': 'job:cancelled', 'job': '123/2/3', 'user': 'john'}]

To post a new activity event::

    >>> event = {'event': 'job:completed', 'job': '123/2/4', 'user': 'john'}
    >>> project.activity.add(event)

Or post multiple events at once::

    >>> events = [
        {'event': 'job:completed', 'job': '123/2/5', 'user': 'john'},
        {'event': 'job:cancelled', 'job': '123/2/6', 'user': 'john'},
    ]
    >>> project.activity.add(events)


Collections
-----------

As an example, let's store hash and timestamp pair for foo spider.

Usual workflow with `Collections`_ would be::

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

Collections are available on project level only.

Frontiers
---------

Typical workflow with `Frontier`_::

    >>> frontiers = project.frontiers

Get all frontiers from a project to iterate through it::

    >>> frontiers.iter()
    <list_iterator at 0x103c93630>

List all frontiers::

    >>> frontiers.list()
    ['test', 'test1', 'test2']

Get a frontier by name::

    >>> frontier = frontiers.get('test')
    >>> frontier
    <scrapinghub.client.Frontier at 0x1048ae4a8>

Get an iterator to iterate through a frontier slots::

    >>> frontier.iter()
    <list_iterator at 0x1030736d8>

List all slots::

    >>> frontier.list()
    ['example.com', 'example.com2']

Get a frontier slot by name::

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

There are convenient shortcuts: ``f`` for ``fingerprints`` and ``q`` for ``queue``.

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

Tags
----

Tags is a convenient way to mark specific jobs (for better search, postprocessing etc).

To mark a job with tag ``consumed``::

    >>> job.update_tags(add=['consumed'])

To mark all spider jobs with tag ``consumed``::

    >>> spider.jobs.update_tags(add=['consumed'])

To remove existing tag ``existing`` for all spider jobs::

    >>> spider.jobs.update_tags(remove=['existing'])

Modifying tags is available on spider/job levels.


Exceptions
----------

scrapinghub.exceptions.ScrapinghubAPIError
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Base exception class.


scrapinghub.exceptions.InvalidUsage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Usually raised in case of 400 response from API.


scrapinghub.exceptions.NotFound
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Entity doesn't exist (e.g. spider or project).


scrapinghub.exceptions.ValueTooLarge
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Value cannot be writtent because it exceeds size limits.

scrapinghub.exceptions.DuplicateJobError
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Job for given spider with given arguments is already scheduled or running.




Legacy client
=============

First, you connect to Scrapinghub::

    >>> from scrapinghub import Connection
    >>> conn = Connection('APIKEY')
    >>> conn
    Connection('APIKEY')

You can list the projects available to your account::

    >>> conn.project_ids()
    [123, 456]

And select a particular project to work with::

    >>> project = conn[123]
    >>> project
    Project(Connection('APIKEY'), 123)
    >>> project.id
    123

To schedule a spider run (it returns the job id)::

    >>> project.schedule('myspider', arg1='val1')
    u'123/1/1'

To get the list of spiders in the project::

    >>> project.spiders()
    [
      {u'id': u'spider1', u'tags': [], u'type': u'manual', u'version': u'123'},
      {u'id': u'spider2', u'tags': [], u'type': u'manual', u'version': u'123'}
    ]

To get all finished jobs::

    >>> jobs = project.jobs(state='finished')

``jobs`` is a ``JobSet``. ``JobSet`` objects are iterable and, when iterated,
return an iterable of ``Job`` objects, so you typically use it like this::

    >>> for job in jobs:
    ...     # do something with job

Or, if you just want to get the job ids::

    >>> [x.id for x in jobs]
    [u'123/1/1', u'123/1/2', u'123/1/3']

To select a specific job::

    >>> job = project.job(u'123/1/2')
    >>> job.id
    u'123/1/2'

To retrieve all scraped items from a job::

    >>> for item in job.items():
    ...     # do something with item (it's just a dict)

To retrieve all log entries from a job::

    >>> for logitem in job.log():
    ...     # logitem is a dict with logLevel, message, time

To get job info::

    >>> job.info['spider']
    'myspider'
    >>> job.info['started_time']
    '2010-09-28T15:09:57.629000'
    >>> job.info['tags']
    []
    >>> job.info['fields_count]['description']
    1253

To mark a job with tag ``consumed``::

    >>> job.update(add_tag='consumed')

To mark several jobs with tag ``consumed`` (``JobSet`` also supports the
``update()`` method)::

    >>> project.jobs(state='finished').update(add_tag='consumed')

To delete a job::

    >>> job.delete()

To delete several jobs (``JobSet`` also supports the ``update()`` method)::

    >>> project.jobs(state='finished').delete()


Legacy Hubstorage client
========================

The library can also be used for interaction with spiders, jobs and scraped data through ``storage.scrapinghub.com`` endpoints.

First, use your API key for authorization::

    >>> from scrapinghub import HubstorageClient
    >>> hc = HubstorageClient(auth='apikey')
    >>> hc.server_timestamp()
    1446222762611

Project
-------

To get project settings or jobs summary::

    >>> project = hc.get_project('1111111')
    >>> project.settings['botgroups']
    [u'botgroup1', ]
    >>> project.jobsummary()
    {u'finished': 6,
     u'has_capacity': True,
     u'pending': 0,
     u'project': 1111111,
     u'running': 0}

Spider
------

To get spider id correlated with its name::

    >>> project.ids.spider('foo')
    1

To see last jobs summaries::

    >>> summaries = project.spiders.lastjobsummary(count=3)

To get job summary per spider::

    >>> summary = project.spiders.lastjobsummary(spiderid='1')

Job
---

Job can be **retrieved** directly by id (project_id/spider_id/job_id)::

    >>> job = hc.get_job('1111111/1/1')
    >>> job.key
    '1111111/1/1'
    >>> job.metadata['state']
    u'finished'

**Creating** a new job requires a spider name::

    >>> job = hc.push_job(projectid='1111111', spidername='foo')
    >>> job.key
    '1111111/1/1'

Priority can be between 0 and 4 (from lowest to highest), the default is 2.

To push job from project level with the highest priority::

    >>> job = project.push_job(spidername='foo', priority=4)
    >>> job.metadata['priority']
    4

Pushing a job with spider arguments::

    >>> project.push_job(spidername='foo', spider_args={'arg1': 'foo', 'arg2': 'bar'})

Running job can be **cancelled** by calling ``request_cancel()``::

    >>> job.request_cancel()
    >>> job.metadata['cancelled_by']
    u'John'

To **delete** job::

    >>> job.purged()
    >>> job.metadata['state']
    u'deleted'

Job details
-----------

Job details can be found in jobs metadata and it's scrapystats::

    >>> job = hc.get_job('1111111/1/1')
    >>> job.metadata['version']
    u'5123a86-master'
    >>> job.metadata['scrapystats']
    ...
    u'downloader/response_count': 104,
    u'downloader/response_status_count/200': 104,
    u'finish_reason': u'finished',
    u'finish_time': 1447160494937,
    u'item_scraped_count': 50,
    u'log_count/DEBUG': 157,
    u'log_count/INFO': 1365,
    u'log_count/WARNING': 3,
    u'memusage/max': 182988800,
    u'memusage/startup': 62439424,
    ...

Anything can be stored in metadata, here is example how to add tags::

    >>> job.update_metadata({'tags': 'obsolete'})

Jobs
----

To iterate through all jobs metadata per project (descending order)::

    >>> jobs_metadata = project.jobq.list()
    >>> [j['key'] for j in jobs_metadata]
    ['1111111/1/3', '1111111/1/2', '1111111/1/1']

Jobq metadata fieldset is less detailed, than ``job.metadata``, but contains few new fields as well.
Additional fields can be requested using the ``jobmeta`` parameter.
If it used, then it's up to the user to list all the required fields, so only few default fields would be added except requested ones::

    >>> metadata = next(project.jobq.list())
    >>> metadata.get('spider', 'missing')
    u'foo'
    >>> jobs_metadata = project.jobq.list(jobmeta=['scheduled_by', ])
    >>> metadata = next(jobs_metadata)
    >>> metadata.get('scheduled_by', 'missing')
    u'John'
    >>> metadata.get('spider', 'missing')
    missing

By default ``jobq.list()`` returns maximum last 1000 results. Pagination is available using the ``start`` parameter::

    >>> jobs_metadata = project.jobq.list(start=1000)

There are several filters like spider, state, has_tag, lacks_tag, startts and endts.
To get jobs filtered by tags::

    >>> jobs_metadata = project.jobq.list(has_tag=['new', 'verified'], lacks_tag='obsolete')

List of tags has ``OR`` power, so in the case above jobs with 'new' or 'verified' tag are expected.

To get certain number of last finished jobs per some spider::

    >>> jobs_metadata = project.jobq.list(spider='foo', state='finished' count=3)

There are 4 possible job states, which can be used as values for filtering by state:

- pending
- running
- finished
- deleted


Items
-----

To iterate through items::

    >>> items = job.items.iter_values()
    >>> for item in items:
    # do something, item is just a dict

Logs
----

To iterate through 10 first logs for example::

    >>> logs = job.logs.iter_values(count=10)
    >>> for log in logs:
    # do something, log is a dict with log level, message and time keys

Collections
-----------

Let's store hash and timestamp pair for foo spider. Usual workflow with `Collections`_ would be::

    >>> collections = project.collections
    >>> foo_store = collections.new_store('foo_store')
    >>> foo_store.set({'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'})
    >>> foo_store.count()
    1
    >>> foo_store.get('002d050ee3ff6192dcbecc4e4b4457d7')
    {u'value': u'1447221694537'}
    >>> # iterate over _key & value pair
    ... list(foo_store.iter_values())
    [{u'_key': u'002d050ee3ff6192dcbecc4e4b4457d7', u'value': u'1447221694537'}]
    >>> # filter by multiple keys - only values for keys that exist will be returned
    ... list(foo_store.iter_values(key=['002d050ee3ff6192dcbecc4e4b4457d7', 'blah']))
    [{u'_key': u'002d050ee3ff6192dcbecc4e4b4457d7', u'value': u'1447221694537'}]
    >>> foo_store.delete('002d050ee3ff6192dcbecc4e4b4457d7')
    >>> foo_store.count()
    0

Frontier
--------

Typical workflow with `Frontier`_::

    >>> frontier = project.frontier

Add a request to the frontier::

    >>> frontier.add('test', 'example.com', [{'fp': '/some/path.html'}])
    >>> frontier.flush()
    >>> frontier.newcount
    1

Add requests with additional parameters::

    >>> frontier.add('test', 'example.com', [{'fp': '/'}, {'fp': 'page1.html', 'p': 1, 'qdata': {'depth': 1}}])
    >>> frontier.flush()
    >>> frontier.newcount
    2

To delete the slot ``example.com`` from the frontier::

    >>> frontier.delete_slot('test', 'example.com')

To retrieve requests for a given slot::

    >>> reqs = frontier.read('test', 'example.com')

To delete a batch of requests::

    >>> frontier.delete('test', 'example.com', '00013967d8af7b0001')

To retrieve fingerprints for a given slot::

    >>> fps = [req['requests'] for req in frontier.read('test', 'example.com')]

Tests
=====

The package is covered with integration tests based on `VCR.py library`_: there
are recorded cassettes files in ``tests/*/cassettes`` used instead of HTTP
requests to real services, it helps to simplify and speed up development.

By default, tests use VCR.py ``once`` mode to:

- replay previously recorded interactions.
- record new interactions if there is no cassette file.
- cause an error to be raised for new requests if there is a cassette file.

It means that if you add new integration tests and run all tests as usual,
only new cassettes will be created, all existing cassettes will stay unmodified.

To ignore existing cassettes and use real service, please provide a flag::

    py.test --ignore-cassettes

If you want to update/recreate all the cassettes from scratch, please use::

    py.test --update-cassettes

Note that internally the above command erases the whole folder with cassettes.


.. _Scrapinghub API: http://doc.scrapinghub.com/api.html
.. _Collections: http://doc.scrapinghub.com/api/collections.html
.. _Frontier: http://doc.scrapinghub.com/api/frontier.html
.. _VCR.py library: https://pypi.python.org/pypi/vcrpy
.. _count endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-count
.. _list endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-list
