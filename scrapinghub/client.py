import json

from requests import session
from requests.exceptions import HTTPError
from requests.compat import urlencode
from requests.compat import urljoin

from scrapinghub.hubstorage import HubstorageClient

from scrapinghub.hubstorage.client import Projects as _Projects
from scrapinghub.hubstorage.project import Project as _Project
from scrapinghub.hubstorage.project import Spiders as _Spiders
from scrapinghub.hubstorage.job import Job as _Job
from scrapinghub.hubstorage.job import JobMeta as _JobMeta
from scrapinghub.hubstorage.jobq import JobQ as _JobQ
from scrapinghub.hubstorage.jobq import DuplicateJobError


class ScrapinghubAPIError(Exception):
    pass


class ScrapinghubClient(HubstorageClient):

    DEFAULT_DASH_ENDPOINT = 'https://app.scrapinghub.com/api/'

    def __init__(self, auth=None, endpoint=None, dash_endpoint=None, **kwargs):
        # listing first kwargs of original class to keep order for main args
        super(self.__class__, self).__init__(
            auth=auth, endpoint=endpoint, **kwargs)

        self.dash_endpoint = dash_endpoint or self.DEFAULT_DASH_ENDPOINT
        # FIXME in this way we define jobq & projects twice, but it's cleaner
        # than copy-pasting full __init__ body of original class
        self.jobq = JobQ(self, None)
        self.projects = Projects(self, None)

    def get_job(self, *args, **kwargs):
        # same logic but with different Job class
        return Job(self, *args, **kwargs)

    def push_job(self, *args, **kwargs):
        # FIXME could be also implemented via class empty property
        # push_job = property()
        raise AttributeError(
            "Scheduling jobs from client level is deprecated."
            "Please schedule new jobs via project.push_job()."
        )

    def project_ids(self):
        """Returns a list of available projects."""
        return self.projects.list()


class DashMixin(object):
    """A simple mixin class to work with Dash API endpoints"""

    def _dash_apiget(self, endpoint, basepath, params=None, raw=False,
                     auth=None, **kwargs):
        """Performs GET request to Dash endpoint.
        Thin wrapper over requests.session.get.
        """
        url = urljoin(endpoint, basepath)
        if params:
            url = "{0}?{1}".format(url, urlencode(params, True))
        response = self.client.session.get(
            url, auth=auth or self.auth, **kwargs)
        return self._decode_dash_response(response, raw)

    def _dash_apipost(self, endpoint, basepath, raw=False, auth=None,
                      **kwargs):
        """Performs POST request to Dash endpoint.
        Thin wrapper over requests.session.post.
        """
        url = urljoin(endpoint, basepath)
        response = self.client.session.post(
            url, auth=auth or self.auth, **kwargs)
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
        # same logic but with different Project class
        return Project(self.client, *args, **kwargs)

    def list(self):
        return self._dash_apiget(self.client.dash_endpoint,
                                 'scrapyd/listprojects.json')

class Project(_Project, DashMixin):

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.jobq = JobQ(self.client, self.projectid, auth=self.auth)
        self.spiders = Spiders(self.client, self.projectid, auth=self.auth)

    def push_job(self, spidername, **jobparams):
        # same logic but with different Job class
        data = self.jobq.push(spidername, **jobparams)
        key = data['jobid']
        return Job(self.client, key, auth=self.auth)


class Spiders(_Spiders, DashMixin):

    def list(self):
        # FIXME ResourceType doesn't store key as is, but we can extract it
        # easily from local key itself, though ofc it's a rude hack
        params = {'project': self.key.split('/')[-1]}
        result = self._dash_apiget(self.client.dash_endpoint,
                                   'spiders/list.json',
                                   params=params)
        return result['spiders']

    def update_tags(self, spidername, add=None, remove=None):
        # FIXME extracting project using splitting spiders/ID key is hack-ish
        projectid = self.key.split('/')[-1]
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params['spider'] = "noop"
        params['project'] = self.key.split('/')[-1]
        response = self._dash_apipost(self.client.dash_endpoint,
                                      'jobs/update.json', data=params)


class Job(_Job):

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        # FIXME a bit rude hack to reuse wrong self.metadata
        # to get client and cached data
        self.metadata = JobMeta(self.metadata.client,
                                self.key, self.auth,
                                cached=self.metadata._cached)

    def update_tags(self, *args, **kwargs):
        return self.metadata.update_tags(*args, **kwargs)


class JobMeta(_JobMeta, DashMixin):

    def update_tags(self, add=None, remove=None):
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params['job'] = self.key.split('/', 1)[1]
        params['project'] = params['job'].split('/', 1)[0]
        response = self._dash_apipost(self.client.dash_endpoint,
                                      'jobs/update.json', data=params)


class JobQ(_JobQ, DashMixin):

    def push(self, spider, **jobparams):
        jobparams['spider'] = spider
        # FIXME ResourceType doesn't store key as is, but we can extract it
        # from url itself but only if it's called from Project.JobQ
        # for Client-level JobQ project should be provided via jobparams
        if 'project' not in jobparams and self.url != 'jobq':
            jobparams['project'] = self.url.rsplit('/')[-1]
        # FIXME JobQ endpoint can schedule multiple jobs with json-lines,
        # corresponding Dash endpoint that we're going to use supports
        # only one job per request: most likely we need to extend Dash
        # endpoint functionality to work in the same manner.
        response = self._dash_apipost(self.client.dash_endpoint,
                                      'schedule.json',
                                      data=jobparams)
        if response.get('status') == 'error':
            if 'already scheduled' in response['message']:
                raise DuplicateJobError(response['message'])
            raise ScrapinghubAPIError(response['message'])
        return response


def get_tags_for_update(**kwargs):
    """Helper to check tags changes"""
    params = {}
    for k, v in kwargs.items():
        if not v:
            continue
        if not isinstance(v, list):
            raise ScrapinghubAPIError("Add/remove value must be a list")
        params[k] = v
    return params
