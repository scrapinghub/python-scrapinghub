====================================
Client interface for Scrapinghub API
====================================

.. image:: https://secure.travis-ci.org/scrapinghub/python-scrapinghub.png?branch=master
   :target: http://travis-ci.org/scrapinghub/python-scrapinghub

Requirements
============

* Python 2.6 or above
* `Requests`_ library

Usage
=====

The ``scrapinghub`` module is a Python library for communicating with the
`Scrapinghub API`_.

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

.. _Scrapinghub API: http://doc.scrapinghub.com/api.html
.. _Requests: http://docs.python-requests.org/
