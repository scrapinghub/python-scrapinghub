from __future__ import absolute_import

from ..hubstorage.activity import Activity as _Activity
from ..hubstorage.collectionsrt import Collections as _Collections
from ..hubstorage.project import Settings as _Settings

from .activity import Activity
from .collections import Collections
from .frontiers import _HSFrontier, Frontiers
from .jobs import Jobs
from .proxy import _MappingProxy
from .spiders import Spiders
from .utils import parse_project_id


class Projects(object):
    """Collection of projects available to current user.

    Not a public constructor: use :class:`~scrapinghub.client.ScrapinghubClient`
    client instance to get a :class:`Projects` instance.
    See :attr:`scrapinghub.client.Scrapinghub.projects` attribute.

    Usage::

        >>> client.projects
        <scrapinghub.client.projects.Projects at 0x1047ada58>
    """

    def __init__(self, client):
        self._client = client

    def get(self, project_id):
        """Get project for a given project id.

        :param project_id: integer or string numeric project id.
        :return: a project object.
        :rtype: :class:`Project`

        Usage::

            >>> project = client.projects.get(123)
            >>> project
            <scrapinghub.client.projects.Project at 0x106cdd6a0>
        """
        return Project(self._client, parse_project_id(project_id))

    def list(self):
        """Get list of projects available to current user.

        :return: a list of project ids.
        :rtype: :class:`list[int]`

        Usage::

            >>> client.projects.list()
            [123, 456]
        """
        return self._client._connection.project_ids()

    def iter(self):
        """Iterate through list of projects available to current user.

        Provided for the sake of API consistency.

        :return: an iterator over project ids list.
        :rtype: :class:`collections.Iterable[int]`
        """
        return iter(self.list())

    def summary(self, state=None, **params):
        """Get short summaries for all available user projects.

        :param state: a string state or a list of states.
        :return: a list of dictionaries: each dictionary represents a project
            summary (amount of pending/running/finished jobs and a flag if it
            has a capacity to run new jobs).
        :rtype: :class:`list[dict]`

        Usage::

            >>> client.projects.summary()
            [{'finished': 674,
              'has_capacity': True,
              'pending': 0,
              'project': 123,
              'running': 1},
             {'finished': 33079,
              'has_capacity': True,
              'pending': 0,
              'project': 456,
              'running': 2}]
        """
        if state:
            params['state'] = state
        return self._client._hsclient.projects.jobsummaries(**params)


class Project(object):
    """Class representing a project object and its resources.

    Not a public constructor: use :class:`~scrapinghub.client.ScrapinghubClient`
    instance or :class:`Projects` instance to get a :class:`Project` instance.
    See :meth:`scrapinghub.client.ScrapinghubClient.get_project` or
    :meth:`Projects.get` methods.

    :ivar key: string project id.
    :ivar activity: :class:`~scrapinghub.client.activity.Activity` resource object.
    :ivar collections: :class:`~scrapinghub.client.collections.Collections` resource object.
    :ivar frontiers: :class:`~scrapinghub.client.frontiers.Frontiers` resource object.
    :ivar jobs: :class:`~scrapinghub.client.jobs.Jobs` resource object.
    :ivar settings: :class:`~scrapinghub.client.settings.Settings` resource object.
    :ivar spiders: :class:`~scrapinghub.client.spiders.Spiders` resource object.

    Usage::

        >>> project = client.get_project(123)
        >>> project
        <scrapinghub.client.projects.Project at 0x106cdd6a0>
        >>> project.key
        '123'
    """

    def __init__(self, client, project_id):
        self.key = str(project_id)
        self._client = client

        # sub-resources
        self.jobs = Jobs(client, project_id)
        self.spiders = Spiders(client, project_id)

        # proxied sub-resources
        self.activity = Activity(_Activity, client, project_id)
        self.collections = Collections(_Collections, client, project_id)
        self.frontiers = Frontiers(_HSFrontier, client, project_id)
        self.settings = Settings(_Settings, client, project_id)


class Settings(_MappingProxy):
    """Class representing job metadata.

    Not a public constructor: use :class:`Project` instance to get a
    :class:`Settings` instance. See :attr:`Project.settings` attribute.

    Usage:

    - get project settings instance::

        >>> project.settings
        <scrapinghub.client.projects.Settings at 0x10ecf1250>

    - iterate through project settings::

        >>> project.settings.iter()
        <dictionary-itemiterator at 0x10ed11578>

    - list project settings::

        >>> project.settings.list()
        [(u'default_job_units', 2), (u'job_runtime_limit', 20)]

    - get setting value by name::

        >>> project.settings.get('default_job_units')
        2

    - update setting value (some settings are read-only)::

        >>> project.settings.set('default_job_units', 2)

    - update multiple settings at once::

        >>> project.settings.update({'default_job_units': 1,
        ...                          'job_runtime_limit': 20})

    - delete project setting by name::

        >>> project.settings.delete('job_runtime_limit')
    """
    def set(self, key, value):
        """Update project setting value by key.

        :param key: a string setting key.
        :param value: new setting value.
        """
        # FIXME drop the method when post-by-key is implemented on server side
        self.update({key: value})
