from scrapinghub import Connection as _Connection
from scrapinghub import HubstorageClient as _HubstorageClient

from .exceptions import _wrap_http_errors
from .projects import Projects
from .utils import parse_auth
from .utils import parse_project_id, parse_job_key


__all__ = ['ScrapinghubClient']

DEFAULT_CONNECTION_TIMEOUT = 60


class Connection(_Connection):

    @_wrap_http_errors
    def _request(self, *args, **kwargs):
        return super(Connection, self)._request(*args, **kwargs)


class HubstorageClient(_HubstorageClient):

    @_wrap_http_errors
    def request(self, *args, **kwargs):
        return super(HubstorageClient, self).request(*args, **kwargs)


class ScrapinghubClient(object):
    """Main class to work with Scrapinghub API.

    :param auth: (optional) Scrapinghub APIKEY or other SH auth credentials.
        If not provided, it will read, respectively, from 
        ``SH_APIKEY`` or ``SHUB_JOBAUTH`` environment variables.
        ``SHUB_JOBAUTH`` is available by default in *Scrapy Cloud*, but it does
        not provide access to all endpoints (e.g. job scheduling), but it is allowed
        to access job data, collections, crawl frontier.
        If you need full access to *Scrapy Cloud* features, you'll need to
        provide a Scrapinghub APIKEY through this argument or deploying ``SH_APIKEY``.
    :param dash_endpoint: (optional) Scrapinghub Dash panel url.
    :param \*\*kwargs: (optional) Additional arguments for
        :class:`~scrapinghub.hubstorage.HubstorageClient` constructor.

    :ivar projects: projects collection,
        :class:`~scrapinghub.client.projects.Projects` instance.

    Usage::

        >>> from scrapinghub import ScrapinghubClient
        >>> client = ScrapinghubClient('APIKEY')
        >>> client
        <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>
    """

    def __init__(self, auth=None, dash_endpoint=None,
                 connection_timeout=DEFAULT_CONNECTION_TIMEOUT, **kwargs):
        self.projects = Projects(self)
        login, password = parse_auth(auth)
        timeout = connection_timeout or DEFAULT_CONNECTION_TIMEOUT
        self._connection = Connection(apikey=login,
                                      password=password,
                                      url=dash_endpoint,
                                      connection_timeout=timeout)
        self._hsclient = HubstorageClient(auth=(login, password),
                                          connection_timeout=timeout, **kwargs)

    def get_project(self, project_id):
        """Get :class:`scrapinghub.client.projects.Project` instance with
        a given project id.

        The method is a shortcut for client.projects.get().

        :param project_id: integer or string numeric project id.
        :return: a project instance.
        :rtype: :class:`~scrapinghub.client.projects.Project`

        Usage::

            >>> project = client.get_project(123)
            >>> project
            <scrapinghub.client.projects.Project at 0x106cdd6a0>
        """
        return self.projects.get(parse_project_id(project_id))

    def get_job(self, job_key):
        """Get :class:`~scrapinghub.client.jobs.Job` with a given job key.

        :param job_key: job key string in format ``project_id/spider_id/job_id``,
            where all the components are integers.
        :return: a job instance.
        :rtype: :class:`~scrapinghub.client.jobs.Job`

        Usage::

            >>> job = client.get_job('123/1/1')
            >>> job
            <scrapinghub.client.jobs.Job at 0x10afe2eb1>
        """
        project_id = parse_job_key(job_key).project_id
        return self.projects.get(project_id).jobs.get(job_key)

    def close(self, timeout=None):
        """Close client instance.

        :param timeout: (optional) float timeout secs to stop gracefully.
        """
        self._hsclient.close(timeout=timeout)
