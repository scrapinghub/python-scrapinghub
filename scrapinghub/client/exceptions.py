# -*- coding: utf-8 -*-
from __future__ import absolute_import, annotations
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from requests import HTTPError

from ..legacy import APIError

_P = ParamSpec('_P')
_T = TypeVar('_T')


def _get_http_error_msg(exc: BaseException | None) -> str:
    if isinstance(exc, HTTPError) and exc.response is not None:
        try:
            payload = exc.response.json()
        except ValueError:
            payload = None
        if payload and isinstance(payload, dict):
            message: str | None = payload.get('message')
            if message:
                return message
        elif exc.response.text:
            return exc.response.text
    return str(exc)


class ScrapinghubAPIError(Exception):
    """Base exception class."""

    def __init__(self, message: object = None,
                 http_error: BaseException | None = None) -> None:
        self.http_error = http_error
        if not message:
            message = _get_http_error_msg(http_error)
        super(ScrapinghubAPIError, self).__init__(message)


class BadRequest(ScrapinghubAPIError):
    """Usually raised in case of 400 response from API."""


class Unauthorized(ScrapinghubAPIError):
    """Request lacks valid authentication credentials for the target resource."""


class Forbidden(ScrapinghubAPIError):
    """You don't have the permission to access the requested resource.
    It is either read-protected or not readable by the server."""


class NotFound(ScrapinghubAPIError):
    """Entity doesn't exist (e.g. spider or project)."""


class ValueTooLarge(ScrapinghubAPIError):
    """Value cannot be writtent because it exceeds size limits."""


class DuplicateJobError(ScrapinghubAPIError):
    """Job for given spider with given arguments is already scheduled or running."""


class ServerError(ScrapinghubAPIError):
    """Indicates some server error: something unexpected has happened."""


def _wrap_http_errors(method: Callable[_P, _T]) -> Callable[_P, _T]:
    """Internal helper to handle exceptions gracefully."""
    @wraps(method)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        try:
            return method(*args, **kwargs)
        except HTTPError as exc:
            assert exc.response is not None
            status_code = exc.response.status_code
            if status_code == 400:
                raise BadRequest(http_error=exc)
            elif status_code == 401:
                raise Unauthorized(http_error=exc)
            elif status_code == 403:
                raise Forbidden(http_error=exc)
            elif status_code == 404:
                raise NotFound(http_error=exc)
            elif status_code == 413:
                raise ValueTooLarge(http_error=exc)
            elif 400 <= status_code < 500:
                raise ScrapinghubAPIError(http_error=exc)
            elif 500 <= status_code < 600:
                raise ServerError(http_error=exc)
            raise
        except APIError as exc:
            msg = exc.args[0]
            if exc._type == APIError.ERR_NOT_FOUND:
                raise NotFound(msg)
            elif exc._type == APIError.ERR_VALUE_ERROR:
                raise ValueError(msg)
            elif exc._type == APIError.ERR_BAD_REQUEST:
                raise BadRequest(msg)
            elif exc._type == APIError.ERR_AUTH_ERROR:
                raise Unauthorized(http_error=exc)
            elif exc._type == APIError.ERR_SERVER_ERROR:
                raise ServerError(http_error=exc)
            raise ScrapinghubAPIError(msg)
    return wrapped
