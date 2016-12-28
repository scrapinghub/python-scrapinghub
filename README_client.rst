===========================================
[Beta] Client interface for Scrapinghub API
===========================================


The ``scrapinghub.ScrapinghubClient`` is a new Python client for communicating
with the `Scrapinghub API`_. It takes best from ``scrapinghub.Connection`` and
``scrapinghub.HubstorageClient`` and combines it under single interface.


.. contents:: :depth: 3


Usage
=====

First, you connect to Scrapinghub::

    >>> from scrapinghub import ScrapinghubClient
    >>> client = ScrapinghubClient('APIKEY')
    >>> client
    <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>

Client instance has ``projects`` field for access to client projects collection.

Projects (client level)
-----------------------

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

(The above is a shortcut for ``client.projects.get(123)``.)

Project (projects level)
------------------------

Project instance has ``jobs`` field to work with the project jobs.

To schedule a spider run (it returns a job object)::

    >>> project.jobs.schedule('spider1', arg1='val1')
    <scrapinghub.client.Job at 0x106ee12e8>>

(Check ``Jobs`` section below for other features.)

Project instance also has the following fields:

- activity - access to project activity records
- collections - work with project collections (see ``Collections`` section)
- frontier - using project frontier (see ``Frontier`` section)
- reports - work with project reports
- settings - interface to project settings
- spiders - access to spiders collection (see ``Spiders`` section)


Spiders (project level)
-----------------------

To get the list of spiders in the project::

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

Spider (spiders level)
----------------------

Like project instance, spider instance has ``jobs`` field to work with the spider's jobs.

To schedule a spider run (you don't need to specify spider name explicitly)::

    >>> spider.jobs.schedule(arg1='val1')
    <scrapinghub.client.Job at 0x106ee12e8>>

Jobs (project/spider level)
---------------------------

To select a specific job for a project::

    >>> job = project.jobs.get('123/1/2')
    >>> job.id
    '123/1/2'

Also there's a shortcut to get same job with client instance::

    >>> job = client.get_job('123/1/2')

Use ``schedule`` method to schedule a new job for project/spider::

    >>> job = spider.jobs.schedule()

It's possible to count jobs for a given project/spider::

    >> spider.jobs.count()
    5

Count logic supports different filters, as described for `count endpoint`_.

To get a list of jobs for a spider::

    >>> jobs = spider.jobs.iter()

Iter logic also supports different filters, as described for `list endpoint`_.

For example, to get all finished jobs::

    >>> jobs = spider.jobs.iter(state='finished')

``jobs`` is an iterator and, when iterated, return an iterable of dict objects,
so you typically use it like this::

    >>> for job in jobs:
    ...     # do something with job data

Or, if you just want to get the job ids::

    >>> [x['key'] for x in jobs]
    ['123/1/1', '123/1/2', '123/1/3']

Job dictionary object itself looks like::

    >>> job
    {
      'key': '123/1/2',
      'spider': 'myspider',
      'version': 'some-version'
      'state': 'finished',
      'close_reason': 'success',
      'errors': 0,
      'logs': 8,
      'pending_time': 1482852737072,
      'running_time': 1482852737848,
      'finished_time': 1482852774356,
      'ts': 1482852755902,
      'elapsed': 207609,
    }

Dict entries returned by ``iter`` method contain some additional meta, but can be
easily converted to ``Job`` instances with::

    >>> [Job(x['key']) for x in jobs]
    [
      <scrapinghub.client.Job at 0x106e2cc18>,
      <scrapinghub.client.Job at 0x106e260b8>,
      <scrapinghub.client.Job at 0x106e26a20>,
    ]

To check jobs summary::

    >>> spider.jobs.summary()

    [{'count': 0, 'name': 'pending', 'summary': []},
     {'count': 0, 'name': 'running', 'summary': []},
     {'count': 5,
      'name': 'finished',
      'summary': [..,

It's also possible to get last job summary (for each spider)::

    >>> list(sp.jobs.lastjobsummary())
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

(Note that there can be a lot of spiders, so the method above returns an iterator.)

Job (jobs level)
----------------

To delete a job::

    >>> job.delete()



To get job metadata::

    >>> job.metadata['spider']
    'myspider'
    >>> job.metadata['started_time']
    '2010-09-28T15:09:57.629000'
    >>> job.metadata['tags']
    []
    >>> j.metadata['scrapystats']['memusage/max']
    53628928

Items (job level)
-----------------

To retrieve all scraped items from a job::

    >>> for item in job.items.iter():
    ...     # do something with item (it's just a dict)

Logs (job level)
----------------

To retrieve all log entries from a job::

    >>> for logitem in job.logs.iter():
    ...     # logitem is a dict with level, message, time
    >>> logitem
    {
      'level': 20,
      'message': '[scrapy.core.engine] Closing spider (finished)',
      'time': 1482233733976},
    }

Requests (job level)
--------------------

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


Additional features
===================

Collections (project level)
---------------------------

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

Frontier (project level)
------------------------

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


Tags (spider/job level)
-----------------------

Tags is a convenient way to mark specific jobs (for better search, postprocessing etc).

To mark a job with tag ``consumed``::

    >>> job.update_tags(add=['consumed'])

To mark all spider jobs with tag ``consumed``::

    >>> spider.update_tags(add=['consumed'])

To remove existing tag ``existing`` for all spider jobs::

    >>> spider.update_tags(remove=['existing'])

.. _count endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-count
.. _list endpoint: https://doc.scrapinghub.com/api/jobq.html#jobq-project-id-list
.. _Collections: http://doc.scrapinghub.com/api/collections.html
.. _Frontier: http://doc.scrapinghub.com/api/frontier.html
