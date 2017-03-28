from __future__ import absolute_import

from requests.compat import urljoin

from .exceptions import NotFound, _wrap_http_errors
from .jobs import Jobs
from .utils import get_tags_for_update


class Spiders(object):
    """Class to work with a collection of project spiders.

    Not a public constructor: use :class:`~scrapinghub.client.projects.Project`
    instance to get a :class:`Spiders` instance.
    See :attr:`~scrapinghub.client.projects.Project.spiders` attribute.

    :ivar project_id: string project id.

    Usage::

        >>> project.spiders
        <scrapinghub.client.spiders.Spiders at 0x1049ca630>
    """

    def __init__(self, client, project_id):
        self.project_id = project_id
        self._client = client

    def get(self, spider, **params):
        """Get a spider object for a given spider name.

        The method gets/sets spider id (and checks if spider exists).

        :param spider: a string spider name.
        :return: a spider object.
        :rtype: :class:`scrapinghub.client.spiders.Spider`

        Usage::

            >>> project.spiders.get('spider2')
            <scrapinghub.client.spiders.Spider at 0x106ee3748>
            >>> project.spiders.get('non-existing')
            NotFound: Spider non-existing doesn't exist.
        """
        project = self._client._hsclient.get_project(self.project_id)
        spider_id = project.ids.spider(spider, **params)
        if spider_id is None:
            raise NotFound("Spider {} doesn't exist.".format(spider))
        return Spider(self._client, self.project_id, spider_id, spider)

    def list(self):
        """Get a list of spiders for a project.

        :return: a list of dictionaries with spiders metadata.
        :rtype: :class:`list[dict]`

        Usage::

            >>> project.spiders.list()
            [{'id': 'spider1', 'tags': [], 'type': 'manual', 'version': '123'},
             {'id': 'spider2', 'tags': [], 'type': 'manual', 'version': '123'}]
        """
        project = self._client._connection[self.project_id]
        return project.spiders()

    def iter(self):
        """Iterate through a list of spiders for a project.

        :return: an iterator over spiders list where each spider is represented
            as a dict containing its metadata.
        :rtype: :class:`collection.Iterable[dict]`

        Provided for the sake of API consistency.
        """
        return iter(self.list())


class Spider(object):
    """Class representing a Spider object.

    Not a public constructor: use :class:`Spiders` instance to get
    a :class:`Spider` instance. See :meth:`Spiders.get` method.

    :ivar project_id: a string project id.
    :ivar key: a string key in format 'project_id/spider_id'.
    :ivar name: a spider name string.
    :ivar jobs: a collection of jobs, :class:`~scrapinghub.client.jobs.Jobs` object.

    Usage::

        >>> spider = project.spiders.get('spider1')
        >>> spider.key
        '123/1'
        >>> spider.name
        'spider1'
    """

    def __init__(self, client, project_id, spider_id, spider):
        self.project_id = project_id
        self.key = '{}/{}'.format(str(project_id), str(spider_id))
        self._id = str(spider_id)
        self.name = spider
        self.jobs = Jobs(client, project_id, self)
        self._client = client

    @_wrap_http_errors
    def update_tags(self, add=None, remove=None):
        """Update tags for the spider.

        :param add: (optional) a list of string tags to add.
        :param remove: (optional) a list of string tags to remove.
        """
        params = get_tags_for_update(add=add, remove=remove)
        path = 'v2/projects/{}/spiders/{}/tags'.format(self.project_id,
                                                       self._id)
        url = urljoin(self._client._connection.url, path)
        response = self._client._connection._session.patch(url, json=params)
        response.raise_for_status()

    @_wrap_http_errors
    def list_tags(self):
        """List spider tags.

        :return: a list of spider tags.
        :rtype: :class:`list[str]`
        """
        path = 'v2/projects/{}/spiders/{}'.format(self.project_id, self._id)
        url = urljoin(self._client._connection.url, path)
        response = self._client._connection._session.get(url)
        response.raise_for_status()
        return response.json().get('tags', [])
