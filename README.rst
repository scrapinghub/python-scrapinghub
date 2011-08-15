====================================
Client interface for Scrapinghub API
====================================

The ``scrapinghub`` module is a Python library for communicating with the
:ref:`Scrapinghub API <api>`.

You can download it from the following link (you may want to right click and
select "Save As"):

This documentation contains some examples to illustrate how to use the
``scrapinghub`` module. For more information see: :ref:`api`.


First, you connect to Scrapinghub::

    >>> from scrapinghub import Connection
    >>> conn = Connection('http://panel.scrapinghub.com/api/', 'user', 'password')
    >>> conn
    Connection(http://panel.scrapinghub.com/api/)

You can list the projects available to your account::

    >>> conn.project_names()
    [u'123', u'456']

And select a particular project to work with::

    >>> project = conn['123']
    >>> project
    Project(Connection(http://panel.scrapinghub.com/api/), '123')
    >>> project.name
    '123'


If you want to schedule a spider::

    >>> project.schedule('groupon.com', arg1='val1')
    [u'4ca37770a1a3a24c45000005', u'4ca33330a1a3a24c45000005']

To get all finished jobs::

    >>> jobs = project.jobs(state='finished')

``jobs`` is a ``JobSet``. ``JobSet`` objects are iterable and, when iterated,
return an iterable of ``Job`` objects, so you typically use it like this::

    >>> for job in jobs:
    ...     # do something with job

Or, if you just want to get the job ids::

    >>> [x.id for x in jobs]
    [u'4c916f80e8bd6f68c2000000', u'4c9170fae8bd6f6cac000000', u'4c9190bde8bd6f761c000000']

To select a specific job::

    >>> job = project.job('4cdacfe7a1a3a27d7a000000')
    >>> job.id
    '4cdacfe7a1a3a27d7a000000'

To retrieve all scraped items from a job::

    >>> for item in job.items():
    ...     # do something with item (it's just a dict)

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
