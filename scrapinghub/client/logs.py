from __future__ import absolute_import

import json
import logging

from .proxy import _ItemsResourceProxy, _DownloadableProxyMixin
from .utils import LogLevel


class Logs(_DownloadableProxyMixin, _ItemsResourceProxy):
    """Representation of collection of job logs.

    Not a public constructor: use :class:`~scrapinghub.client.jobs.Job` instance
    to get a :class:`Logs` instance. See :attr:`~scrapinghub.client.jobs.Job.logs`
    attribute.

    Please note that :meth:`list` method can use a lot of memory and for a
    large amount of logs it's recommended to iterate through it via :meth:`iter`
    method (all params and available filters are same for both methods).

    Usage:

    - retrieve all logs from a job::

        >>> job.logs.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - iterate through first 100 log entries and print them::

        >>> for log in job.logs.iter(count=100):
        ...     print(log)

    - retrieve a single log entry from a job::

        >>> job.logs.list(count=1)
        [{
            'level': 20,
            'message': '[scrapy.core.engine] Closing spider (finished)',
            'time': 1482233733976,
        }]

    - retrive logs with a given log level and filter by a word::

        >>> filters = [("message", "contains", ["mymessage"])]
        >>> job.logs.list(level='WARNING', filter=filters)
        [{
            'level': 30,
            'message': 'Some warning: mymessage',
            'time': 1486375511188,
        }]
    """
    def log(self, message, level=logging.INFO, ts=None, **other):
        """Base method to write a log entry.

        :param message: a string message.
        :param level: (optional) logging level, default to INFO.
        :param ts: (optional) UNIX timestamp in milliseconds.
        :param \*\*other: other optional kwargs.
        """
        self._origin.log(message, level=level, ts=ts, **other)

    def debug(self, message, **other):
        """Log a message with DEBUG level."""
        self._origin.debug(message, **other)

    def info(self, message, **other):
        """Log a message with INFO level."""
        self._origin.info(message, **other)

    def warn(self, message, **other):
        """Log a message with WARN level."""
        self._origin.warn(message, **other)
    warning = warn

    def error(self, message, **other):
        """Log a message with ERROR level."""
        self._origin.error(message, **other)

    def batch_write_start(self):
        """Override to set a start parameter when commencing writing."""
        return self._origin.batch_write_start()

    def _modify_iter_params(self, params):
        """Modify iter() filters on-the-fly.

        - convert offset to start parameter
        - check log level and create a corresponding meta filter

        :param params: an original dictionary with params.
        :return: a modified dictionary with params.
        :rtype: :class:`dict`
        """
        params = super(Logs, self)._modify_iter_params(params)
        offset = params.pop('offset', None)
        if offset:
            params['start'] = '{}/{}'.format(self.key, offset)
        level = params.pop('level', None)
        if level:
            minlevel = getattr(LogLevel, level, None)
            if minlevel is None:
                raise ValueError("Unknown log level: {}".format(level))
            level_filter = json.dumps(['level', '>=', [minlevel]])
            # there can already be some filters handled by super class method
            params['filter'] = params.get('filter', []) + [level_filter]
        return params
