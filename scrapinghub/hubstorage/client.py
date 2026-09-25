"""
High level Hubstorage client
"""
from __future__ import annotations

import logging
import os
from typing import Any

from requests import session, HTTPError, ConnectionError, Response, Session, Timeout
from retrying import Retrying
from .utils import _Auth, _Part, xauth, urlpathjoin
from .project import Project
from .job import Job
from .jobq import JobQ
from .batchuploader import BatchUploader
from .resourcetype import ResourceType
from .serialization import MSGPACK_AVAILABLE


__all__ = ["HubstorageClient"]

logger = logging.getLogger('HubstorageClient')

_HTTP_ERROR_CODES_TO_RETRY = (408, 429, 502, 503, 504)


def _hc_retry_on_exception(err: BaseException) -> bool:
    """Callback used by the client to restrict the retry to acceptable errors"""
    if (isinstance(err, HTTPError) and err.response is not None
            and err.response.status_code in _HTTP_ERROR_CODES_TO_RETRY):
        logger.warning("Server failed with %d status code, retrying (maybe)", err.response.status_code)
        return True

    if isinstance(err, ConnectionError):
        logger.warning("Request encountered a connection error: %r, retrying (maybe)", err)
        return True

    if isinstance(err, Timeout):
        logger.warning("Server connection timeout, retrying (maybe)")
        return True

    return False


def _get_package_version() -> str:
    """Small helper to avoid circular imports"""
    from scrapinghub import __version__
    return __version__


class _JobQClientProxy:

    def __init__(self, client: HubstorageClient, endpoint: str) -> None:
        self.auth = client.auth
        self.endpoint = endpoint
        self.use_msgpack = False
        self.request = client.request


class HubstorageClient(object):

    DEFAULT_ENDPOINT = 'https://storage.scrapinghub.com/'
    DEFAULT_USER_AGENT = 'python-scrapinghub/{version}'.format(
        version=_get_package_version())

    DEFAULT_CONNECTION_TIMEOUT_S = 60.0
    RETRY_DEFAUT_MAX_RETRY_TIME_S = 60.0

    RETRY_DEFAULT_MAX_RETRIES = 3
    RETRY_DEFAULT_JITTER_MS = 500
    RETRY_DEFAULT_EXPONENTIAL_BACKOFF_MS = 500

    def __init__(self, auth: _Auth = None, endpoint: str | None = None,
                 connection_timeout: float | None = None,
                 max_retries: int | None = None,
                 max_retry_time: float | None = None,
                 user_agent: str | None = None, use_msgpack: bool = True, *,
                 jobq_endpoint: str | None = None) -> None:
        """
        Note:
            max_retries and max_retry_time change how the client attempt to retry failing requests that are
            idempotent (safe to execute multiple time).

            HubstorageClient(max_retries=3) will retry requests 3 times, no matter the time it takes.
            Use max_retry_time if you want to bound the time spent in retrying.

            By default, requests are retried at most 3 times, during 60 seconds.

        Args:
            auth (str): The client authentication token
            endpoint (str, optional): The API root address. If not provided, it will be read from the ``SHUB_STORAGE`` environment variable, or fall back to ``"https://storage.scrapinghub.com/"``.
            connection_timeout (int): The connection timeout for a _single request_
            max_retries (int): The number of time idempotent requests may be retried
            max_retry_time (int): The time, in seconds, during which the client can retry a request
            use_msgpack (bool): Flag to enable/disable msgpack use for serialization
            jobq_endpoint (str, optional): The JobQ API root address.
                Keyword-only argument. If not provided, it will be read from
                the ``SHUB_JOBQ`` environment variable, or fall back to the
                value of ``endpoint``.
        """
        self.auth = xauth(auth)
        self.endpoint = endpoint or os.environ.get("SHUB_STORAGE", self.DEFAULT_ENDPOINT)
        self._jobq_endpoint = (
            jobq_endpoint or
            os.getenv("SHUB_JOBQ") or
            self.endpoint
        )
        self.connection_timeout = connection_timeout or self.DEFAULT_CONNECTION_TIMEOUT_S
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.session = self._create_session()
        self.retrier = self._create_retrier(max_retries, max_retry_time)
        self._jobq_client = _JobQClientProxy(self, self._jobq_endpoint)
        self.jobq = JobQ(self._jobq_client, None)
        self.projects = Projects(self, None)
        self.root: ResourceType = ResourceType(self, None)
        self._batchuploader: BatchUploader | None = None
        self.use_msgpack: bool = MSGPACK_AVAILABLE and use_msgpack
        if use_msgpack != self.use_msgpack:
            logger.warning('Messagepack is not available, please ensure that '
                           'msgpack library is properly installed.')

    def request(self, is_idempotent: bool = False, **kwargs: Any) -> Response:
        """
        Execute an HTTP request with the current client session.

        Use the retry policy configured in the client when is_idempotent is True
        """
        kwargs.setdefault('timeout', self.connection_timeout)

        def invoke_request() -> Response:
            r = self.session.request(**kwargs)

            try:
                r.raise_for_status()
                return r
            except HTTPError:
                logger.debug('%s: %s', r, r.content)
                raise

        if is_idempotent:
            response: Response = self.retrier.call(invoke_request)
            return response
        else:
            return invoke_request()

    def _create_retrier(self, max_retries: int | None,
                        max_retry_time: float | None) -> Retrying:
        """
        Create the Retrier object used to process idempotent client requests.

        If only max_retries is set, the default max_retry_time is ignored.

        Args:
            max_retries (int): the number of retries to be attempted
            max_retry_time (int): the number of time, in seconds, to retry for.
        Returns:
            A Retrying instance, that implements a call(func) method.
        """

        # Client sets max_retries only
        if max_retries is not None and max_retry_time is None:
            stop_max_delay: float | None = None
            stop_max_attempt_number = max_retries + 1
            wait_exponential_multiplier: float = self.RETRY_DEFAULT_EXPONENTIAL_BACKOFF_MS
        else:
            stop_max_delay = (max_retry_time or self.RETRY_DEFAUT_MAX_RETRY_TIME_S) * 1000.0
            stop_max_attempt_number = (max_retries or self.RETRY_DEFAULT_MAX_RETRIES) + 1

            # Compute the backoff to allow for max_retries queries during the allowed delay
            # Solves the following formula (assumes requests are immediate):
            # max_retry_time = sum(exp_multiplier * 2 ** i) for i from 1 to max_retries + 1
            wait_exponential_multiplier = stop_max_delay / ((2 ** (stop_max_attempt_number + 1)) - 2)

        return Retrying(stop_max_attempt_number=stop_max_attempt_number,
                        stop_max_delay=stop_max_delay,
                        retry_on_exception=_hc_retry_on_exception,
                        wait_exponential_multiplier=wait_exponential_multiplier,
                        wait_jitter_max=self.RETRY_DEFAULT_JITTER_MS)

    def _create_session(self) -> Session:
        s = session()
        s.headers.update({'User-Agent': self.user_agent})
        return s

    @property
    def batchuploader(self) -> BatchUploader:
        if self._batchuploader is None:
            self._batchuploader = BatchUploader(self)
        return self._batchuploader

    def get_job(self, *args: Any, **kwargs: Any) -> Job:
        return Job(self, *args, **kwargs)

    def push_job(self, projectid: _Part, spidername: str, auth: _Auth = None,
                 **jobparams: Any) -> Job:
        project = self.projects.get(projectid, auth=auth)
        return project.push_job(spidername, **jobparams)

    def get_project(self, *args: Any, **kwargs: Any) -> Project:
        return self.projects.get(*args, **kwargs)

    def server_timestamp(self) -> Any:
        tsurl = urlpathjoin(self.endpoint, 'system/ts')
        return self.session.get(tsurl).json()

    def close(self, timeout: float | None = None) -> None:
        if self._batchuploader is not None:
            self.batchuploader.close(timeout)


class Projects(ResourceType):

    resource_type = 'projects'

    client: HubstorageClient

    def get(self, *args: Any, **kwargs: Any) -> Project:
        return Project(self.client, *args, **kwargs)

    def jobsummaries(self, auth: _Auth = None, **params: Any) -> Any:
        auth = xauth(auth) or self.auth
        return next(self.apiget('jobsummaries', params=params, auth=auth))
