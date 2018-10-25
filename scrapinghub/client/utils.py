from __future__ import absolute_import

import os
import json
import logging
import binascii
import warnings
from codecs import decode

import six


class LogLevel(object):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    SILENT = CRITICAL + 1


class JobKey(object):

    def __init__(self, project_id, spider_id, job_id):
        self.project_id = project_id
        self.spider_id = spider_id
        self.job_id = job_id

    def __str__(self):
        return '{}/{}/{}'.format(self.project_id, self.spider_id, self.job_id)


def parse_project_id(project_id):
    """Simple check for project id.

    :param project_id: a numeric project id, int or string.
    :return: a unified project id.
    :rtype: :class:`str`
    """
    try:
        int(project_id)
    except ValueError:
        raise ValueError("Project id should be convertible to integer")
    return str(project_id)


def parse_job_key(job_key):
    """Inner helper to parse job key.

    :param job_key: a job key (str or tuple of 3 ints).
    :return: parsed job key.
    :rtype: :class:`JobKey`
    """
    if isinstance(job_key, tuple):
        parts = job_key
    elif isinstance(job_key, six.string_types):
        parts = job_key.split('/')
    else:
        raise ValueError("Job key should be a string or a tuple")
    if len(parts) != 3:
        raise ValueError(
            "Job key should consist of project_id/spider_id/job_id")
    try:
        list(map(int, parts))
    except ValueError:
        raise ValueError("Job key parts should be integers")
    return JobKey(*map(str, parts))


def get_tags_for_update(**kwargs):
    """Helper to check tags changes"""
    params = {}
    for k, v in kwargs.items():
        if not v:
            continue
        if not isinstance(v, list):
            raise ValueError("Add/remove field value must be a list")
        params[k] = v
    return params


def update_kwargs(kwargs, **params):
    """Update kwargs dict with non-empty params with json-encoded values."""
    kwargs.update({k: json.dumps(v) if isinstance(v, dict) else v
                   for k, v in params.items() if v is not None})


def parse_auth(auth):
    """Parse authentification token.

    >>> os.environ['SH_APIKEY'] = 'apikey'
    >>> parse_auth(None)
    ('apikey', '')
    >>> parse_auth(('user', 'pass'))
    ('user', 'pass')
    >>> parse_auth('user:pass')
    ('user', 'pass')
    >>> parse_auth('c3a3c298c2b8c3a6c291c284c3a9')
    ('c3a3c298c2b8c3a6c291c284c3a9', '')
    >>> parse_auth('312f322f333a736f6d652e6a77742e746f6b656e')
    ('1/2/3', 'some.jwt.token')
    """
    if auth is None:
        apikey = os.environ.get('SH_APIKEY')
        if apikey:
            return (apikey, '')

        jobauth = os.environ.get('SHUB_JOBAUTH')
        if jobauth:
            warnings.warn("You are using the SHUB_JOBAUTH environment "
                          "variable which may not work for some API endpoints")
            return _search_for_jwt_credentials(jobauth)

        raise RuntimeError("No API key provided and neither SH_APIKEY "
                           "nor SHUB_JOBAUTH environment variables is set")

    if isinstance(auth, tuple):
        all_strings = all(isinstance(k, six.string_types) for k in auth)
        if len(auth) != 2 or not all_strings:
            raise ValueError("Wrong authentication credentials")
        return auth

    if not isinstance(auth, six.string_types):
        raise ValueError("Wrong authentication credentials")

    jwt_auth = _search_for_jwt_credentials(auth)
    if jwt_auth:
        return jwt_auth

    login, _, password = auth.partition(':')
    return (login, password)


def _search_for_jwt_credentials(auth):
    try:
        decoded_auth = decode(auth, 'hex_codec')
    except (binascii.Error, TypeError):
        return
    try:
        if not isinstance(decoded_auth, six.string_types):
            decoded_auth = decoded_auth.decode('ascii')
        login, _, password = decoded_auth.partition(':')
        if password and parse_job_key(login):
            return (login, password)
    except (UnicodeDecodeError, ValueError):
        pass
