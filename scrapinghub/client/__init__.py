from scrapinghub import Connection as _Connection
from scrapinghub import HubstorageClient as _HubstorageClient

from .projects import Projects
from .exceptions import wrap_http_errors

from .utils import parse_auth
from .utils import parse_project_id, parse_job_key


__all__ = ['ScrapinghubClient']


class Connection(_Connection):

    @wrap_http_errors
    def _request(self, *args, **kwargs):
        return super(Connection, self)._request(*args, **kwargs)


class HubstorageClient(_HubstorageClient):

    @wrap_http_errors
    def request(self, *args, **kwargs):
        return super(HubstorageClient, self).request(*args, **kwargs)


class ScrapinghubClient(object):
    """Main class to work with Scrapinghub API.

    :param auth: Scrapinghub APIKEY or other SH auth credentials.
    :param dash_endpoint: (optional) Scrapinghub Dash panel url.
    :param \*\*kwargs: (optional) Additional arguments for
        :class:`scrapinghub.hubstorage.HubstorageClient` constructor.

    :ivar projects: projects collection, :class:`Projects` instance.

    Usage::

        >>> from scrapinghub import ScrapinghubClient
        >>> client = ScrapinghubClient('APIKEY')
        >>> client
        <scrapinghub.client.ScrapinghubClient at 0x1047af2e8>
    """

    def __init__(self, auth=None, dash_endpoint=None, **kwargs):
        self.projects = Projects(self)
        login, password = parse_auth(auth)
        self._connection = Connection(apikey=login,
                                      password=password,
                                      url=dash_endpoint)
        self._hsclient = HubstorageClient(auth=(login, password), **kwargs)

    def get_project(self, projectid):
        """Get :class:`Project` instance with a given project id.

        The method is a shortcut for client.projects.get().

        :param projectid: integer or string numeric project id.
        :return: :class:`Project` object.
        :rtype: scrapinghub.client.Project.

        Usage::

            >>> project = client.get_project(123)
            >>> project
            <scrapinghub.client.Project at 0x106cdd6a0>
        """
        return self.projects.get(parse_project_id(projectid))

    def get_job(self, jobkey):
        """Get Job with a given jobkey.

        :param jobkey: job key string in format 'project/spider/job',
            where all the components are integers.
        :return: :class:`Job` object.
        :rtype: scrapinghub.client.Job.

        Usage::

            >>> job = client.get_job('123/1/1')
            >>> job
            <scrapinghub.client.Job at 0x10afe2eb1>
        """
        projectid = parse_job_key(jobkey).projectid
        return self.projects.get(projectid).jobs.get(jobkey)

    def close(self, timeout=None):
        """Close client instance.

        :param timeout: (optional) float timeout secs to stop everything
            gracefully.
        """
        self._hsclient.close(timeout=timeout)
