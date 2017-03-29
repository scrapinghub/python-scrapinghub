Overview
========

The :class:`~scrapinghub.client.ScrapinghubClient` is a new Python client for
communicating with the `Scrapinghub API`_.
It takes best from :class:`~scrapinghub.legacy.Connection` and
:class:`~scrapinghub.hubstorage.HubstorageClient`, and combines it under single
interface.

First, you instantiate new client::

    >>> from scrapinghub import ScrapinghubClient
    >>> client = ScrapinghubClient('APIKEY')
    >>> client
    <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>

Client instance has :attr:`~scrapinghub.client.ScrapinghubClient.projects` field
for access to client projects.

Projects
--------

You can list the :class:`~scrapinghub.client.projects.Projects` available to your
account::

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

.. tip:: The above is a shortcut for ``client.projects.get(123)``.


Project
-------

:class:`~scrapinghub.client.projects.Project` instance has
:attr:`~scrapinghub.client.projects.Project.jobs` field to work with
the project jobs.

:class:`~scrapinghub.client.jobs.Jobs` instance is described well in
:ref:`Jobs <jobs>` section below.

For example, to schedule a spider run (it returns a
:class:`~scrapinghub.client.jobs.Job` object)::

    >>> project.jobs.run('spider1', job_args={'arg1': 'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>


Spiders
-------

Spiders collection is accessible via :class:`~scrapinghub.client.spiders.Spiders`.

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

.. _spider:

Spider
------

Like project instance, :class:`~scrapinghub.client.spiders.Spider` instance has
``jobs`` field to work with the spider's jobs.

To schedule a spider run::

    >>> spider.jobs.run(job_args={'arg1': 'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>

Note that you don't need to specify spider name explicitly.

.. _jobs:

Jobs
----

:class:`~scrapinghub.client.jobs.Jobs` collection is available on project/spider
level.

get
^^^

To select a specific job for a project::

    >>> job = project.jobs.get('123/1/2')
    >>> job.key
    '123/1/2'

Also there's a shortcut to get same job with client instance::

    >>> job = client.get_job('123/1/2')

run
^^^

Use ``run`` method to run a new job for project/spider::

    >>> job = spider.jobs.run()

Scheduling logic supports different options, like

- **job_args** to provide arguments for the job
- **units** to specify amount of units to run the job
- **job_settings** to pass additional settings for the job
- **priority** to set higher/lower priority of the job
- **add_tag** to create a job with a set of initial tags
- **meta** to pass additional custom metadata

For example, to run a new job for a given spider with custom params::

    >>> job = spider.jobs.run(units=2, job_settings={'SETTING': 'VALUE'}, priority=1,
    ...                       add_tag=['tagA','tagB'], meta={'custom-data': 'val1'})

Note that if you run a job on project level, spider name is required::

    >>> job = project.jobs.run('spider1')

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
    >>> jobs_summary = project.jobs.iter(jobmeta=['scheduled_by'])
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

List of tags in **has_tag** has ``OR`` power, so in the case above jobs with
``new`` or ``verified`` tag are expected (while list of tags in **lacks_tag**
has ``AND`` power).

To get certain number of last finished jobs per some spider::

    >>> jobs_summary = project.jobs.iter(spider='foo', state='finished', count=3)

There are 4 possible job states, which can be used as values
for filtering by state:

- pending
- running
- finished
- deleted

Dictionary entries returned by ``iter`` method contain some additional meta,
but can be easily converted to :class:`~scrapinghub.client.jobs.Job` instances with::

    >>> [Job(client, x['key']) for x in jobs]
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


update_tags
^^^^^^^^^^^

Tags is a convenient way to mark specific jobs (for better search, postprocessing etc).


To mark all spider jobs with tag ``consumed``::

    >>> spider.jobs.update_tags(add=['consumed'])

To remove existing tag ``existing`` for all spider jobs::

    >>> spider.jobs.update_tags(remove=['existing'])

Modifying tags is available on :class:`~scrapinghub.client.spiders.Spider`/
:class:`~scrapinghub.client.jobs.Job` levels.


Job
---

:class:`~scrapinghub.client.jobs.Job` instance provides access to a job data
with the following fields:

- metadata
- items
- logs
- requests
- samples

Request to cancel a job::

    >>> job.cancel()

To delete a job::

    >>> job.delete()

To mark a job with tag ``consumed``::

    >>> job.update_tags(add=['consumed'])

.. _job-metadata:

Metadata
^^^^^^^^

:class:`~scrapinghub.client.jobs.JobMeta` details can be found in jobs metadata
and it's scrapystats::

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

.. _job-items:

Items
^^^^^

To retrieve all scraped items from a job use
:class:`~scrapinghub.client.items.Items`::

    >>> for item in job.items.iter():
    ...     # do something with item (it's just a dict)

.. _job-logs:

Logs
^^^^

To retrieve all log entries from a job use :class:`~scrapinghub.client.logs.Logs`::

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

To retrieve all requests from a job there's :class:`~scrapinghub.client.requests.Requests`::

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

.. _job-samples:

Samples
^^^^^^^

:class:`~scrapinghub.client.samples.Samples` is useful to retrieve all samples
for a job::

    >>> for sample in job.samples.iter():
    ...     # sample is a list with a timestamp and data
    >>> sample
    [1482233732452, 0, 0, 0, 0, 0]


Activity
--------

:class:`~scrapinghub.client.activity.Activity` provides a convenient interface
to project activity events.

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
    ...     {'event': 'job:completed', 'job': '123/2/5', 'user': 'john'},
    ...     {'event': 'job:cancelled', 'job': '123/2/6', 'user': 'john'},
    ... ]
    >>> project.activity.add(events)


Collections
-----------

As an example, let's store hash and timestamp pair for foo spider.

Usual workflow with :class:`~scrapinghub.client.collections.Collections` would be::

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
