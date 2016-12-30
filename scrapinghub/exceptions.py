# -*- coding: utf-8 -*-
from functools import wraps

from requests import HTTPError

from .hubstorage import ValueTooLarge as _ValueTooLarge


def _get_http_error_msg(exc):
    if isinstance(exc, HTTPError):
        try:
            payload = exc.response.json()
        except ValueError:
            payload = None
        if payload and isinstance(payload, dict):
            error = payload.get('error')
            if error:
                return error
        elif exc.response.text:
            return exc.response.text
    return str(exc)


class ScrapinghubAPIError(Exception):

    def __init__(self, message=None, http_error=None):
        self.http_error = http_error
        if not message:
            message = _get_http_error_msg(http_error)
        super(ScrapinghubAPIError, self).__init__(message)


class InvalidUsage(ScrapinghubAPIError):
    pass


class NotFound(ScrapinghubAPIError):
    pass


class ValueTooLarge(ScrapinghubAPIError):
    pass


class DuplicateJobError(ScrapinghubAPIError):
    pass


def wrap_http_errors(method):
    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except HTTPError as exc:
            status_code = exc.response.status_code
            if status_code == 400:
                raise InvalidUsage(http_error=exc)
            elif status_code == 404:
                raise NotFound(http_error=exc)
            elif status_code == 413:
                raise ValueTooLarge(http_error=exc)
            elif 400 <= status_code < 500:
                raise ScrapinghubAPIError(http_error=exc)
            raise
    return wrapped


def wrap_value_too_large(method):
    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except _ValueTooLarge as exc:
            raise ValueTooLarge(str(exc))
    return wrapped
