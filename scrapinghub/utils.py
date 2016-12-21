import logging

from six import string_types

from scrapinghub import APIError


class DuplicateJobError(APIError):
    pass


class WrongProjectID(APIError):
    pass


class WrongJobKey(APIError):
    pass


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
        projectid = int(projectid)
    except ValueError:
        raise WrongProjectID("Project ID should be convertible to integer")
    return projectid


def parse_job_key(jobkey):
    if isinstance(jobkey, tuple):
        parts = jobkey
    elif isinstance(jobkey, string_types):
        parts = jobkey.split('/')
    else:
        raise WrongJobKey("Job key should be a string or a tuple")
    if len(parts) != 3:
        raise WrongJobKey("Job key should consist of projectid/spiderid/jobid")
    try:
        parts = map(int, parts)
    except ValueError:
        raise WrongJobKey("Job key parts should be integers")
    return JobKey(*parts)


def get_tags_for_update(**kwargs):
    """Helper to check tags changes"""
    params = {}
    for k, v in kwargs.items():
        if not v:
            continue
        if not isinstance(v, list):
            raise APIError("Add/remove field value must be a list")
        params[k] = v
    return params


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


def wrap_kwargs(fn, kwargs_fn):
    """Tiny wrapper to prepare modified version of function kwargs"""
    def wrapped(*args, **kwargs):
        kwargs = kwargs_fn(kwargs)
        return fn(*args, **kwargs)
    return wrapped
