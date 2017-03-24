Quickstart
==========

Requirements
------------

* Python 2.7 or above


Installation
------------

The quick way::

    pip install scrapinghub

You can also install the library with MessagePack support, it provides better
response time and improved bandwidth usage::

    pip install scrapinghub[msgpack]


Basic usage
-----------

Instantiate new client::

    >>> from scrapinghub import ScrapinghubClient
    >>> client = ScrapinghubClient('APIKEY')

Work with your projects::

    >>> client.projects.list()
    [123, 456]

Run new jobs from the client::

    >>> project = client.get_project(123)
    >>> project.jobs.run('spider1', job_args={'arg1':'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>

Access your jobs data::

    >>> job = client.get_job('123/1/2')
    >>> for item in job.items():
    ...     print(item)
    {
        'name': ['Some other item'],
        'url': 'http://some-url/other-item.html',
        'size': 35000,
    }

Many more feature are awaiting for you.


Tests
-----

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


.. _VCR.py library: https://pypi.python.org/pypi/vcrpy
