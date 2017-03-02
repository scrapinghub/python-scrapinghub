from __future__ import absolute_import

from requests.compat import urljoin

from .jobs import Jobs
from .exceptions import NotFound
from .exceptions import wrap_http_errors
from .utils import get_tags_for_update


class Spiders(object):
    """Class to work with a collection of project spiders.

    Not a public constructor: use :class:`Project` instance to get
    a :class:`Spiders` instance. See :attr:`Project.spiders` attribute.

    :ivar projectid: integer project id.

    Usage::

        >>> project.spiders
        <scrapinghub.client.Spiders at 0x1049ca630>
    """

    def __init__(self, client, projectid):
        self.projectid = projectid
        self._client = client

    def get(self, spidername, **params):
        """Get a spider object for a given spider name.

        The method gets/sets spider id (and checks if spider exists).

        :param spidername: a string spider name.
        :return: :class:`Spider` object.
        :rtype: scrapinghub.client.Spider.

        Usage::

            >>> project.spiders.get('spider2')
            <scrapinghub.client.Spider at 0x106ee3748>
            >>> project.spiders.get('non-existing')
            NotFound: Spider non-existing doesn't exist.
        """
        project = self._client._hsclient.get_project(self.projectid)
        spiderid = project.ids.spider(spidername, **params)
        if spiderid is None:
            raise NotFound("Spider {} doesn't exist.".format(spidername))
        return Spider(self._client, self.projectid, spiderid, spidername)

    def list(self):
        """Get a list of spiders for a project.

        :return: a list of dictionaries with spiders metadata.

        Usage::  # noqa

            >>> project.spiders.list()
            [{'id': 'spider1', 'tags': [], 'type': 'manual', 'version': '123'},
             {'id': 'spider2', 'tags': [], 'type': 'manual', 'version': '123'}]
        """
        project = self._client._connection[self.projectid]
        return project.spiders()

    def iter(self):
        """Iterate through a list of spiders for a project.

        Provided for the sake of API consistency.
        """
        return iter(self.list())


class Spider(object):
    """Class representing a Spider object.

    Not a public constructor: use :class:`Spiders` instance to get
    a :class:`Spider` instance. See :meth:`Spiders.get` method.

    :ivar projectid: integer project id.
    :ivar name: a spider name string.
    :ivar jobs: a collection of jobs, :class:`Jobs` object.

    Usage::

        >>> spider = project.spiders.get('spider1')
        >>> spider.key
        '123/1'
        >>> spider.name
        'spider1'
    """

    def __init__(self, client, projectid, spiderid, spidername):
        self.projectid = projectid
        self.key = '{}/{}'.format(str(projectid), str(spiderid))
        self._id = str(spiderid)
        self.name = spidername
        self.jobs = Jobs(client, projectid, self)
        self._client = client

    @wrap_http_errors
    def update_tags(self, add=None, remove=None):
        params = get_tags_for_update(add=add, remove=remove)
        path = 'v2/projects/{}/spiders/{}/tags'.format(self.projectid,
                                                       self._id)
        url = urljoin(self._client._connection.url, path)
        response = self._client._connection._session.patch(url, json=params)
        response.raise_for_status()

    @wrap_http_errors
    def list_tags(self):
        path = 'v2/projects/{}/spiders/{}'.format(self.projectid, self._id)
        url = urljoin(self._client._connection.url, path)
        response = self._client._connection._session.get(url)
        response.raise_for_status()
        return response.json().get('tags', [])
