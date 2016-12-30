===========================================
[Beta] Client interface for Scrapinghub API
===========================================


The ``scrapinghub.ScrapinghubClient`` is a new Python client for communicating
with the `Scrapinghub API`_. It takes best from ``scrapinghub.Connection`` and
``scrapinghub.HubstorageClient`` and combines it under single interface.


.. contents:: :depth: 2


Testing
=======

The client is covered with integration tests based on `VCR.py library`_: there
are recorded cassettes files in ``tests/client/cassettes`` used instead of HTTP
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


Basic usage
===========

First, you connect to Scrapinghub::

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
    >>> project.id
    123

The above is a shortcut for ``client.projects.get(123)``.

Project
-------

Project instance has ``jobs`` field to work with the project jobs.

Jobs instance is described well in ``Jobs`` section below.

For example, to schedule a spider run (it returns a job object)::

    >>> project.jobs.schedule('spider1', arg1='val1')
    <scrapinghub.client.Job at 0x106ee12e8>>

Project instance also has the following fields:

- activity - access to project activity records
- collections - work with project collections (see ``Collections`` section)
- frontier - using project frontier (see ``Frontier`` section)
- settings - interface to project settings
- spiders - access to spiders collection (see ``Spiders`` section)


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
    >>> spider.id
    2
    >>> spider.name
    spider2

Spider
------

Like project instance, spider instance has ``jobs`` field to work with the spider's jobs.

To schedule a spider run::

    >>> spider.jobs.schedule(arg1='val1')
    <scrapinghub.client.Job at 0x106ee12e8>>

Note that you don't need to specify spider name explicitly.

Jobs
----

Jobs collection is available on project/spider level.

get
^^^

To select a specific job for a project::

    >>> job = project.jobs.get('123/1/2')
    >>> job.id
    '123/1/2'

Also there's a shortcut to get same job with client instance::

    >>> job = client.get_job('123/1/2')

schedule
^^^^^^^^

Use ``schedule`` method to schedule a new job for project/spider::

    >>> job = spider.jobs.schedule()

Scheduling logic supports different options, like

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

    >> spider.jobs.count()
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
      'summary': [..,

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

    >>> job.metadata['version']
    '5123a86-master'
    >>> job.metadata['scrapystats']
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

    >>> job.update_metadata({'tags': 'obsolete'})

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


Additional features
===================

Collections
-----------

As an example, let's store hash and timestamp pair for foo spider.

Usual workflow with `Collections`_ would be::

    >>> collections = project.collections
    >>> foo_store = collections.new_store('foo_store')
    >>> foo_store.set({'_key': '002d050ee3ff6192dcbecc4e4b4457d7', 'value': '1447221694537'})
    >>> foo_store.count()
    1
    >>> foo_store.get('002d050ee3ff6192dcbecc4e4b4457d7')
    '1447221694537'
    >>> for result in foo_store.iter_values():
    # do something with _key & value pair
    >>> foo_store.delete('002d050ee3ff6192dcbecc4e4b4457d7')
    >>> foo_store.count()
    0

Collections are available on project level only.

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

Frontier is available on project level only.

Tags
----

Tags is a convenient way to mark specific jobs (for better search, postprocessing etc).

To mark a job with tag ``consumed``::

    >>> job.update_tags(add=['consumed'])

To mark all spider jobs with tag ``consumed``::

    >>> spider.update_tags(add=['consumed'])

To remove existing tag ``existing`` for all spider jobs::

    >>> spider.update_tags(remove=['existing'])

Modifying tags is available on spider/job levels.

Exceptions
==========

scrapinghub.exceptions.ScrapinghubAPIError
------------------------------------------

Base exception class.


scrapinghub.exceptions.InvalidUsage
-----------------------------------

Usually raised in case of 400 response from API.


scrapinghub.exceptions.NotFound
-------------------------------

Entity doesn't exist (e.g. spider or project).


scrapinghub.exceptions.ValueTooLarge
------------------------------------

Value cannot be writtent because it exceeds size limits.

scrapinghub.exceptions.DuplicateJobError
----------------------------------------

Job for given spider with given arguments is already scheduled or running.


.. _Scrapinghub API: http://doc.scrapinghub.com/api.html
.. _VCR.py library: https://pypi.python.org/pypi/vcrpy
.. _count endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-count
.. _list endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-list
.. _Collections: http://doc.scrapinghub.com/api/collections.html
.. _Frontier: http://doc.scrapinghub.com/api/frontier.html
