Quickstart
==========

Requirements
------------

* Python 3.10+


Installation
------------

The quick way::

    pip install scrapinghub

It is recommended to install the library with `MessagePack`_ support,
it provides better response time and improved bandwidth usage::

    pip install scrapinghub[msgpack]


Basic usage
-----------

Instantiate a new client with your Scrapy Cloud API key::

    >>> from scrapinghub import ScrapinghubClient
    >>> apikey = '84c87545607a4bc0****************' # your API key as a string
    >>> client = ScrapinghubClient(apikey)

.. note:: Your Scrapy Cloud API key is available at the bottom of
    https://app.zyte.com/o/settings after you sign up.

List your deployed projects::

    >>> client.projects.list()
    [123, 456]

Run a new job for one of your projects::

    >>> project = client.get_project(123)
    >>> project.jobs.run('spider1', job_args={'arg1': 'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>

Access your job's output data::

    >>> job = client.get_job('123/1/2')
    >>> for item in job.items.iter():
    ...     print(item)
    {
        'name': ['Some item'],
        'url': 'http://some-url/item.html',
        'value': 25,
    }
    {
        'name': ['Some other item'],
        'url': 'http://some-url/other-item.html',
        'value': 35,
    }
    ...

Checkout all the other features in :ref:`overview` or in the more
detailed :ref:`api-reference`.


.. _MessagePack: https://en.wikipedia.org/wiki/MessagePack
