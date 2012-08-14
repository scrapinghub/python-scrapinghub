Hub Storage client library
==========================

.. note:: This module is experimental and its API may change without previous
   notice.

This is the HubStorage client library, which contains:

* basic client api (hubstorage.Client)
* scrapy integration (hubstorage.scrapyitems & hubstorage.scrapylog)
* a simple command line tool (hubstorage.tool)
* a tool to convert log files from log.json API format to HubStorage format
  (hubstorage.shlog2jl)

Requirements
------------

* requests: http://pypi.python.org/pypi/requests

Basic API
---------

Example for writing::

    from hubstorage import Client
    client = Client()
    with client.open_item_writer('items/1/2/3') as writer:
        for item in items:
            write.write_item(item)

Example for reading::

    from hubstorage import Client
    client = Client()
    for item in client.iter_items('items/1/2/3'):
        print item

Command line tool
-----------------

Some example usage:

To dump the items of a job in jsonlines format::

    python -m hubstorage.tool --dump items/53/34/7

To upload a jsonlines file into a job::

    cat items.jl | python -m hubstorage.tool --load items/53/34/7

To upload a jsonlines file into a job using `pipe viewer`_ to monitor progress
and throughput::

    pv -l items.jl | python -m hubstorage.tool --load items/53/34/7

Scrapy integration
------------------

The Scrapy integration code assumes you have the following environment
variables defined within the Scrapy process:

* SHUB_PROJECT
* SHUB_SPIDER
* SHUB_JOB
* SHUB_JOBAUTH

These are used to construct the respective HubStorage url paths, and perform
authentication.

To store items in HubStorage enable the following extension::

    EXTENSIONS = {
        'hubstorage.scrapyitems.HubStorage': 1,
    }

To store logs in HubStorage enable the HubStorage log observer with::

    from hubstorage.scrapylog import initialize_hubstorage_logging
    initialize_hubstorage_logging()

.. _pipe viewer: http://www.ivarch.com/programs/pv.shtml
