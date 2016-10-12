====================================
Client interface for Scrapinghub API
====================================

.. image:: https://secure.travis-ci.org/scrapinghub/python-scrapinghub.png?branch=master
   :target: http://travis-ci.org/scrapinghub/python-scrapinghub


The ``scrapinghub`` is a Python library for communicating with the `Scrapinghub API`_.


.. contents:: :depth: 1


Requirements
============

* Python 2.6 or above


Installation
============

The quick way::

    pip install scrapinghub

You can also install the library with MessagePack support, it provides better
response time and improved bandwidth usage::

    pip install scrapinghub[msgpack]


Usage
=====

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


HubstorageClient
================

The library can also be used for interaction with spiders, jobs and scraped data through ``storage.scrapinghub.com`` endpoints.

First, use your API key for authorization::

    >>> from scrapinghub import HubstorageClient
    >>> hс = HubstorageClient(auth='apikey')
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
    '1447221694537'
    >>> for result in foo_store.iter_values():
    # do something with _key & value pair
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



.. _Scrapinghub API: http://doc.scrapinghub.com/api.html
.. _Collections: http://doc.scrapinghub.com/api/collections.html
.. _Frontier: http://doc.scrapinghub.com/api/frontier.html
