import os
import json
import logging
import binascii

from codecs import decode
from six import string_types, binary_type


class LogLevel(object):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    SILENT = CRITICAL + 1


class JobKey(object):

    def __init__(self, projectid, spiderid, jobid):
        self.projectid = projectid
        self.spiderid = spiderid
        self.jobid = jobid

    def __str__(self):
        return '{}/{}/{}'.format(self.projectid, self.spiderid, self.jobid)


def parse_project_id(projectid):
    try:
        int(projectid)
    except ValueError:
        raise ValueError("Project id should be convertible to integer")
    return str(projectid)


def parse_job_key(jobkey):
    if isinstance(jobkey, tuple):
        parts = jobkey
    elif isinstance(jobkey, string_types):
        parts = jobkey.split('/')
    else:
        raise ValueError("Job key should be a string or a tuple")
    if len(parts) != 3:
        raise ValueError("Job key should consist of projectid/spiderid/jobid")
    try:
        map(int, parts)
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


def wrap_kwargs(fn, kwargs_fn):
    """Tiny wrapper to prepare modified version of function kwargs"""
    def wrapped(*args, **kwargs):
        kwargs = kwargs_fn(kwargs)
        return fn(*args, **kwargs)
    return wrapped


def proxy_methods(origin, successor, methods):
    """A helper to proxy methods from origin to successor.

    Accepts a list with strings and tuples:
    - each string defines:
        a successor method name to proxy 1:1 with origin method
    - each tuple should consist of 2 strings:
        a successor method name and an origin method name
    """
    for method in methods:
        if isinstance(method, tuple):
            successor_name, origin_name = method
        else:
            successor_name, origin_name = method, method
        if not hasattr(successor, successor_name):
            setattr(successor, successor_name, getattr(origin, origin_name))


def format_iter_filters(params):
    """Format iter() filter param on-the-fly.

    Support passing multiple filters at once as a list with tuples.
    """
    filters = params.get('filter')
    if filters and isinstance(filters, list):
        filter_data = []
        for elem in params.pop('filter'):
            if isinstance(elem, string_types):
                filter_data.append(elem)
            elif isinstance(elem, (list, tuple)):
                filter_data.append(json.dumps(elem))
            else:
                raise ValueError(
                    "Filter condition must be string, tuple or list")
        if filter_data:
            params['filter'] = filter_data
    return params


def parse_auth(auth):
    """Parse authentification token.

    >>> parse_auth(None)  # noqa
    RuntimeError: No API key provided and SH_APIKEY environment variable not set
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
        if apikey is None:
            raise RuntimeError("No API key provided and SH_APIKEY "
                               "environment variable not set")
        return (apikey, '')

    if isinstance(auth, tuple):
        if len(auth) != 2:
            raise ValueError("Wrong amount of authentication credentials")
        if not isinstance(auth[0], string_types):
            raise ValueError("Login must me of a string type")
        if not (auth[1] is None or isinstance(auth[1], string_types)):
            raise ValueError("Password must be None or of a string type")
        return auth

    if not isinstance(auth, string_types):
        raise ValueError("Wrong authentication credentials")

    decoded_auth = None
    try:
        decoded_auth = decode(auth, 'hex_codec')
    except (binascii.Error, TypeError):
        login, _, password = auth.partition(':')
        if not password:
            raise ValueError("Bad apikey, please check your credentials")
        return (login, password)

    try:
        if not isinstance(decoded_auth, string_types):
            decoded_auth = decoded_auth.decode('ascii')
        login, _, password = decoded_auth.partition(':')
        if password and parse_job_key(login):
            return (login, password)
    except (UnicodeDecodeError, ValueError):
        pass

    return (auth, '')
