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
   :maxdepth: 2

   overview
   client_apidocs


Legacy clients
==============

.. toctree::
   :maxdepth: 2

   legacy_connection
   legacy_hubstorage


Tests
=====

.. toctree::
   :maxdepth: 2

   testing


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
