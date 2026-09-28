============
Contributing
============

python-scrapinghub is an open-source project. Your contribution is very welcome!

Issue Tracker
=============

If you have a bug report, a new feature proposal or simply would like to make
a question, please check our issue tracker on Github:
https://github.com/scrapinghub/python-scrapinghub/issues

Source code
===========

Our source code is hosted on Github:
https://github.com/scrapinghub/python-scrapinghub

Before opening a pull request, it might be worth checking current and previous
issues. Some code changes might also require some discussion before being
accepted so it might be worth opening a new issue before implementing huge or
breaking changes.

Testing
=======

The package is covered with integration tests based on `VCR.py`_ library: there
are recorded cassettes files in ``tests/*/cassettes`` used instead of HTTP
requests to real services, it helps to simplify and speed up development.

By default, tests use VCR.py ``once`` mode to:

- replay previously recorded interactions.
- record new interactions if there is no cassette file.
- cause an error to be raised for new requests if there is a cassette file.

It means that if you add new integration tests and run all tests as usual,
only new cassettes will be created, all existing cassettes will stay unmodified.

To ignore existing cassettes and use real services, please provide a flag::

    py.test --ignore-cassettes

If you want to update/recreate all the cassettes from scratch, please use::

    py.test --update-cassettes

Note that internally the above command erases the whole folder with cassettes.

.. _VCR.py: https://pypi.python.org/pypi/vcrpy
