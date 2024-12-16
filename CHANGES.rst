Release notes
=============

2.5.0 (2024-12-16)
------------------

- remove official support for Python 2.7, 3.5, 3.6 and 3.7
- add official support for Python 3.11 and 3.12
- use the ``SHUB_APIURL`` and ``SHUB_STORAGE`` environment variables as default
  values when available
- fix the warning when ``msgpack`` is not installed (used to say
  ``msgpack-python``)

2.4.0 (2022-03-10)
------------------

- add Python 3.10 support
- improve ``iter()`` fallback in getting the ``meta`` argument

2.3.1 (2020-03-13)
------------------

- update msgpack dependency
- drop official support for Python 3.4
- improve documentation, address a few PEP8 complaints

2.3.0 (2019-12-17)
------------------

- add `items.list_iter` method to iterate by chunks
- fix retrying logic for HTTP errors
- improve documentation

2.2.1 (2019-08-07)
------------------

- use `jobs.cancel` command name to maintain consistency
- provide basic documentation for the new feature

2.2.0 (2019-08-06)
------------------

- add a command to cancel multiple jobs per call
- normalize and simplify using VCR.py cassettes in tests

2.1.1 (2019-04-25)
------------------

- add Python 3.7 support
- update msgpack dependency
- fix `iter` logic for items/requests/logs
- add `truncate` method to collections
- improve documentation

2.1.0 (2019-01-14)
------------------

- add an option to schedule jobs with custom environment variables
- fallback to `SHUB_JOBAUTH` environment variable if `SH_APIKEY` is not set
- provide a unified connection timeout used by both internal clients
- increase a chunk size when working with the items stats endpoint

Python 3.3 is considered unmaintained.

2.0.3 (2017-12-08)
------------------

- fix `iter` logic when applying single `count` param

2.0.2 (2017-12-05)
------------------

- add support for TZ-aware datetime objects
- better tests

2.0.1 (2017-07-19)
------------------

- add a client parameter to disable msgpack use
- add VCR.py json-serialized tests
- make `parent` param optional for requests.add
- improve documentation

2.0.0 (2017-03-29)
------------------

Major release with a lot of new features.

- new powerfull ScrapinghubClient takes best from Connection and HubstorageClient,
  and combines it under single interface
- documentation is available on `Read The Docs (2.0.0)`_

1.9.0 (2016-11-02)
------------------

- `python-hubstorage`_ merged into python-scrapinghub
- all tests are improved and rewritten with py.test
- hubstorage tests use vcrpy cassettes, work faster and don't require any external services to run

`python-hubstorage`_ is going to be considered deprecated,
its next version will contain a deprecation warning and a proposal
to use python-scrapinghub >=1.9.0 instead.

1.8.0 (2016-07-29)
------------------

- python 3 support & unittests
- add retries on httplib.HTTPException
- update scrapinghub api endpoint

1.7.0 (2014-07-25)
------------------

- basic py3.3 compatibility while keeping py2.7 compatibility
- update setup.py classifiers

1.6.2 (2014-07-01)
------------------

- fix travis workaround deploying on tags

1.6.1 (2014-07-01)
------------------

- packaging improvements
- cleaner implementation of project.job()

1.6.0 (2014-03-14)
------------------

- support retreiving a fixed amount of items

1.5.0 (2014-01-29)
------------------

- switch to dash secure endpoint

1.4.4 (2013-12-18)
------------------

- log download failure as error only if all attempts exhausted

1.4.3 (2013-11-25)
------------------

- update travis config to match travis-ci (pypy updated to 2.2)
- update pypi credentials

1.4.2 (2013-11-25)
------------------

- add python 3 to travis-ci matrix

1.4.1 (2013-11-25)
------------------

- tox, travis-ci and pypi uploads
- pypi uploads only on Python 2.7 success
- run tests under pypy 2.1 in travis-ci

1.4.0 (2013-09-04)
------------------

- add bindings for autoscraping api

1.3.0 (2013-08-26)
------------------

- add a way to set starting offset
- suport requesting meta fields

1.2.1 (2013-08-22)
------------------

- resume item downloads on network errors

1.2.0 (2013-08-08)
------------------

- add support for stopping a job
- project.name is deprecated in favour of project.id
- use stricter arguments for Connection constructor
- point to dash.scrapinghub.com api endpoint by default
- enable streaming with requests >= 1.0

1.1.1 (2012-10-24)
------------------

- added automatic retry to items download, when the request fails

1.1 (2012-10-19)
----------------

- report correct version on user-agent string
- ported to uses Requests library (instead of urllib2)
- added support for gzip transfer encoding to increase API throughput on low
  bandwidth connections
- deprecated first url argument of scrapinghub.Connection object
- added support for loading API key from SH_APIKEY environment variable

0.1 (2011-08-15)
----------------

First release of python-scrapinghub.


.. _python-hubstorage: https://github.com/scrapinghub/python-hubstorage
.. _Read The Docs (2.0.0): http://python-scrapinghub.readthedocs.io/en/2.0.0/
