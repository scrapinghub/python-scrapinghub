from requests.compat import urljoin

from .exceptions import _wrap_http_errors


class PeriodicJobs:
    """Class to work with the periodic jobs of a project.

    Not a public constructor: use :class:`~scrapinghub.client.projects.Project`
    instance to get a :class:`PeriodicJobs` instance.
    See :attr:`~scrapinghub.client.projects.Project.periodic_jobs` attribute.

    Periodic jobs are dictionaries with the fields described in the
    :ref:`zyte:api-periodicjobs` reference.
    """

    def __init__(self, client, project_id):
        self.project_id = project_id
        """String project id."""
        self._client = client

    def _url(self, periodic_job_id=None):
        path = 'projects/{}/periodicjobs'.format(self.project_id)
        if periodic_job_id is not None:
            path += '/{}'.format(periodic_job_id)
        return urljoin(self._client._connection.url, path)

    @_wrap_http_errors
    def _request(self, method, url, **kwargs):
        response = self._client._connection._session.request(
            method, url, **kwargs)
        response.raise_for_status()
        return response

    def iter(self):
        """Iterate over the periodic jobs of the project."""
        url = self._url()
        while url:
            data = self._request('GET', url).json()
            for periodic_job in data['results']:
                yield periodic_job
            url = data['next']

    def list(self):
        """Return a list of the periodic jobs of the project."""
        return list(self.iter())

    def get(self, periodic_job_id):
        """Return the periodic job with the given *periodic_job_id*."""
        return self._request('GET', self._url(periodic_job_id)).json()

    def create(self, cron, tasks, **kwargs):
        """Create a periodic job and return it.

        *cron* is a cron expression in UTC, e.g. ``'0 9 * * 1'`` for Mondays at
        09:00. Each field must be either a single value or ``*``.

        *tasks* is a list of dictionaries, one per spider or script to run,
        each with at least a ``name`` key.

        Any other periodic job field, e.g. ``addtags``, ``description`` or
        ``disabled``, can be passed as a keyword argument.
        """
        data = dict(kwargs, cron=cron, tasks=tasks)
        return self._request('POST', self._url(), json=data).json()

    def update(self, periodic_job_id, **kwargs):
        """Update the fields passed as keyword arguments of the periodic job
        with the given *periodic_job_id*, and return the updated periodic job.
        """
        return self._request(
            'PATCH', self._url(periodic_job_id), json=kwargs).json()

    def delete(self, periodic_job_id):
        """Delete the periodic job with the given *periodic_job_id*."""
        self._request('DELETE', self._url(periodic_job_id))
