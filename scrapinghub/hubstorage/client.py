"""
High level Hubstorage client
"""
import logging
from requests import session, HTTPError, ConnectionError, Timeout
from retrying import Retrying
from .utils import xauth, urlpathjoin
from .project import Project
from .job import Job
from .jobq import JobQ
from .batchuploader import BatchUploader
from .resourcetype import ResourceType
from .serialization import MSGPACK_AVAILABLE


__all__ = ["HubstorageClient"]

logger = logging.getLogger('HubstorageClient')

_HTTP_ERROR_CODES_TO_RETRY = (408, 429, 502, 503, 504)


def _hc_retry_on_exception(err):
    """Callback used by the client to restrict the retry to acceptable errors"""
    if isinstance(err, HTTPError) and err.response.status_code in _HTTP_ERROR_CODES_TO_RETRY:
        logger.warning("Server failed with %d status code, retrying (maybe)", err.response.status_code)
        return True

    if isinstance(err, ConnectionError):
        logger.warning("Request encountered a connection error: %r, retrying (maybe)", err)
        return True

    if isinstance(err, Timeout):
        logger.warning("Server connection timeout, retrying (maybe)")
        return True

    return False


def _get_package_version():
    """Small helper to avoid circular imports"""
    from scrapinghub import __version__
    return __version__


class HubstorageClient(object):

    DEFAULT_ENDPOINT = 'https://storage.scrapinghub.com/'
    DEFAULT_USER_AGENT = 'python-scrapinghub/{version}'.format(
        version=_get_package_version())

    DEFAULT_CONNECTION_TIMEOUT_S = 60.0
    RETRY_DEFAUT_MAX_RETRY_TIME_S = 60.0

    RETRY_DEFAULT_MAX_RETRIES = 3
    RETRY_DEFAULT_JITTER_MS = 500
    RETRY_DEFAULT_EXPONENTIAL_BACKOFF_MS = 500

    def __init__(self, auth=None, endpoint=None, connection_timeout=None,
                 max_retries=None, max_retry_time=None, user_agent=None,
                 use_msgpack=True):
        """
        Note:
            max_retries and max_retry_time change how the client attempt to retry failing requests that are
            idempotent (safe to execute multiple time).

            HubstorageClient(max_retries=3) will retry requests 3 times, no matter the time it takes.
            Use max_retry_time if you want to bound the time spent in retrying.

            By default, requests are retried at most 3 times, during 60 seconds.

        Args:
            auth (str): The client authentication token
            endpoint (str): The API root address
            connection_timeout (int): The connection timeout for a _single request_
            max_retries (int): The number of time idempotent requests may be retried
            max_retry_time (int): The time, in seconds, during which the client can retry a request
            use_msgpack (bool): Flag to enable/disable msgpack use for serialization
        """
        self.auth = xauth(auth)
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.connection_timeout = connection_timeout or self.DEFAULT_CONNECTION_TIMEOUT_S
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.session = self._create_session()
        self.retrier = self._create_retrier(max_retries, max_retry_time)
        self.jobq = JobQ(self, None)
        self.projects = Projects(self, None)
        self.root = ResourceType(self, None)
        self._batchuploader = None
        self.use_msgpack = MSGPACK_AVAILABLE and use_msgpack
        if use_msgpack != self.use_msgpack:
            logger.warning('Messagepack is not available, please ensure that '
                           'msgpack-python library is properly installed.')

    def request(self, is_idempotent=False, **kwargs):
        """
        Execute an HTTP request with the current client session.

        Use the retry policy configured in the client when is_idempotent is True
        """
        kwargs.setdefault('timeout', self.connection_timeout)

        def invoke_request():
            r = self.session.request(**kwargs)

            try:
                r.raise_for_status()
                return r
            except HTTPError:
                logger.debug('%s: %s', r, r.content)
                raise

        if is_idempotent:
            return self.retrier.call(invoke_request)
        else:
            return invoke_request()

    def _create_retrier(self, max_retries, max_retry_time):
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
            stop_max_delay = None
            stop_max_attempt_number = max_retries + 1
            wait_exponential_multiplier = self.RETRY_DEFAULT_EXPONENTIAL_BACKOFF_MS
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

    def _create_session(self):
        s = session()
        s.headers.update({'User-Agent': self.user_agent})
        return s

    @property
    def batchuploader(self):
        if self._batchuploader is None:
            self._batchuploader = BatchUploader(self)
        return self._batchuploader

    def get_job(self, *args, **kwargs):
        return Job(self, *args, **kwargs)

    def push_job(self, projectid, spidername, auth=None, **jobparams):
        project = self.projects.get(projectid, auth=auth)
        return project.push_job(spidername, **jobparams)

    def get_project(self, *args, **kwargs):
        return self.projects.get(*args, **kwargs)

    def server_timestamp(self):
        tsurl = urlpathjoin(self.endpoint, 'system/ts')
        return self.session.get(tsurl).json()

    def close(self, timeout=None):
        if self._batchuploader is not None:
            self.batchuploader.close(timeout)


class Projects(ResourceType):

    resource_type = 'projects'

    def get(self, *args, **kwargs):
        return Project(self.client, *args, **kwargs)

    def jobsummaries(self, auth=None, **params):
        auth = xauth(auth) or self.auth
        return next(self.apiget('jobsummaries', params=params, auth=auth))
