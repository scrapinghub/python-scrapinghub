=================================
HubStorage service client library
=================================

.. image:: https://badge.fury.io/py/hubstorage.png
   :target: http://badge.fury.io/py/hubstorage

.. image:: https://secure.travis-ci.org/scrapinghub/python-hubstorage.png?branch=master
   :target: http://travis-ci.org/scrapinghub/python-hubstorage

.. note:: This module is experimental and its API may change without previous
   notice.

Overview
========

This is the HubStorage client library, which contains:

* Full client api trough `hubstorage.HubstorageClient`

Requirements
------------

* requests: http://pypi.python.org/pypi/requests

Basic API
---------

Example creating a new job::

    >>> from hubstorage import HubstorageClient
    >>> hs = HubstorageClient(auth=apikey)
    >>> job = hs.new_job(projectid='1111111', spider='foo')
    >>> job.key
    '1111111/1/1'

    >>> job.metadata['state']
    'pending'

    >>> job.items.write({'title': 'my first item'})
    >>> job.logs.info('lorem impsum message are cool')
    >>> job.logs.error('but sometimes s**t happens')
    >>> job.finished()

Example getting job data later::

    >> job = hs.get_job('1111111/1/1')
    >> job.metadata['state']
    'finished'

    >> list(job.items.list(count=1))
    [{'title': 'my first item'}]

    ...
