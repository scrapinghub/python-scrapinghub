====================================
Client interface for Scrapinghub API
====================================

.. image:: https://secure.travis-ci.org/scrapinghub/python-scrapinghub.png?branch=master
   :target: http://travis-ci.org/scrapinghub/python-scrapinghub

The ``scrapinghub`` is a Python library for communicating with the `Scrapinghub API`_.


.. _Scrapinghub API: http://doc.scrapinghub.com/api.html

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


ScrapinghubClient
=================

.. toctree::
   :maxdepth: 1

   client/overview
   client/apidocs

Legacy clients
==============

.. toctree::
   :maxdepth: 2

   legacy/connection
   legacy/hubstorage


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


.. _VCR.py library: https://pypi.python.org/pypi/vcrpy
