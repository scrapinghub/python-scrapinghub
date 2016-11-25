import json

from requests import session
from requests.exceptions import HTTPError
from requests.compat import urlencode
from requests.compat import urljoin

from scrapinghub.hubstorage import HubstorageClient

from scrapinghub.hubstorage.client import Projects as _Projects
from scrapinghub.hubstorage.project import Project as _Project
from scrapinghub.hubstorage.project import Spiders as _Spiders
from scrapinghub.hubstorage.jobq import JobQ as _JobQ
from scrapinghub.hubstorage.jobq import DuplicateJobError


class ScrapinghubAPIError(Exception):
    pass


class ScrapinghubClient(HubstorageClient):

    DEFAULT_DASH_ENDPOINT = 'https://app.scrapinghub.com/api/'

    def __init__(self, auth=None, endpoint=None, dash_endpoint=None, **kwargs):
        # listing first kwargs to keep order of main arguments
        super(ScrapinghubClient, self).__init__(
            auth=auth, endpoint=endpoint, **kwargs)

        # we should store dash_endpoint and use modified Projects class
        self.dash_endpoint = dash_endpoint or self.DEFAULT_DASH_ENDPOINT
        # FIXME in this way we define self.projects twice, but it's cleaner
        # than copy-pasting __init__ body of original class
        self.jobq = JobQ(self, None)
        self.projects = Projects(self, None)

    def project_ids(self):
        """Returns a list of available projects."""
        return self.projects.list()


class DashMixin(object):
    """A simple mixin class to allow requesting Dash"""

    def _dash_apiget(self, endpoint, basepath, params=None,
                     headers=None, raw=False, auth=None):
        """Performs GET request to SH Dash."""
        url = urljoin(endpoint, basepath)
        if params:
            url = "{0}?{1}".format(url, urlencode(params, True))
        response = self.client.session.get(
            url, headers=headers, auth=auth or self.auth)
        return self._decode_dash_response(response, raw)

    def _dash_apipost(self, endpoint, basepath, params=None,
                      headers=None, raw=False, files=None, auth=None):
        """Performs POST request to SH Dash."""
        url = urljoin(endpoint, basepath)
        response = self.client.session.post(
            url, headers=headers, data=params,
            files=files, auth=auth or self.auth)
        return self._decode_dash_response(response, raw)

    def _decode_dash_response(self, response, raw):
        if raw:
            return response.raw
        data = json.loads(response.text)
        try:
            if data['status'] == 'ok':
                return data
            elif data['status'] in ('error', 'badrequest'):
                raise ScrapinghubAPIError(data['message'])
            raise ScrapinghubAPIError(
                "Unknown response status: {0[status]}".format(data))
        except KeyError:
            raise ScrapinghubAPIError("JSON response does not contain status")


class Projects(_Projects, DashMixin):

    def get(self, *args, **kwargs):
        return Project(self.client, *args, **kwargs)

    def list(self):
        return self._dash_apiget(self.client.dash_endpoint,
                                 'scrapyd/listprojects.json')

class Project(_Project, DashMixin):

    def __init__(self, *args, **kwargs):
        super(Project, self).__init__(*args, **kwargs)
        self.jobq = JobQ(self.client, self.projectid, auth=self.auth)
        self.spiders = Spiders(self.client, self.projectid, auth=self.auth)


class Spiders(_Spiders, DashMixin):

    def list(self):
        # FIXME ResourceType doesn't store key as is, but we can extract it
        # easily from url itself, though ofc it's a rude hack
        params = {'project': self.url.rsplit('/')[-1]}
        result = self._dash_apiget(self.client.dash_endpoint,
                                   'spiders/list.json',
                                   params=params)
        return result['spiders']


class JobQ(_JobQ, DashMixin):

    def push(self, spider, **jobparams):
        # FIXME ResourceType doesn't store key as is, but we can extract it
        # from url itself but only if it's called from Project.JobQ
        # for Client-level JobQ project should be provided via jobparams
        if self.url != 'jobq':
            jobparams['project'] = self.url.rsplit('/')[-1]
        jobparams['spider'] = spider
        # FIXME JobQ endpoint can schedule multiple jobs with json-lines,
        # corresponding Dash endpoint that we're going to use supports
        # only one job per request: most likely we need to extend Dash
        # endpoint functionality to work in the same manner.
        response = self._dash_apipost(self.client.dash_endpoint,
                                      'schedule.json',
                                      params=jobparams)
        if response.get('status') == 'error':
            if 'already scheduled' in response['message']:
                raise DuplicateJobError(response['message'])
            raise ScrapinghubAPIError(response['message'])
        return response
