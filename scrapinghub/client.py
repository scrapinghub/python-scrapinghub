import logging

from requests.compat import urlencode
from requests.compat import urljoin

from scrapinghub.hubstorage import HubstorageClient
from scrapinghub.hubstorage.job import Requests
from scrapinghub.hubstorage.job import Samples as JobSamples
from scrapinghub.hubstorage.jobq import DuplicateJobError
from scrapinghub.hubstorage.project import Ids
from scrapinghub.hubstorage.project import Jobs
from scrapinghub.hubstorage.project import Reports
from scrapinghub.hubstorage.project import Samples as ProjectSamples
from scrapinghub.hubstorage.project import Settings
from scrapinghub.hubstorage.activity import Activity
from scrapinghub.hubstorage.collectionsrt import Collections
from scrapinghub.hubstorage.frontier import Frontier
from scrapinghub.hubstorage.utils import urlpathjoin, xauth

# import the classes to redefine them in the module
from scrapinghub.hubstorage import resourcetype
from scrapinghub.hubstorage.client import Projects as _Projects
from scrapinghub.hubstorage.project import Project as _Project
from scrapinghub.hubstorage.project import Spiders as _Spiders
from scrapinghub.hubstorage.job import Job as _Job
from scrapinghub.hubstorage.job import Items as _Items
from scrapinghub.hubstorage.job import Logs as _Logs
from scrapinghub.hubstorage.job import JobMeta as _JobMeta
from scrapinghub.hubstorage.jobq import JobQ as _JobQ


class ScrapinghubAPIError(Exception):
    pass


class Log(object):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    SILENT = CRITICAL + 1


class ScrapinghubClient(HubstorageClient):

    DEFAULT_DASH_ENDPOINT = 'https://app.scrapinghub.com/api/'

    def __init__(self, auth=None, endpoint=None, dash_endpoint=None,
                 connection_timeout=None, max_retries=None,
                 max_retry_time=None, user_agent=None):
        self.auth = xauth(auth)
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.connection_timeout = (connection_timeout or
                                   self.DEFAULT_CONNECTION_TIMEOUT_S)
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.session = self._create_session()
        self.retrier = self._create_retrier(max_retries, max_retry_time)
        self.jobq = JobQ(self, None)
        self.projects = Projects(self, None)
        self.root = ResourceType(self, None)
        self.dash_endpoint = dash_endpoint or self.DEFAULT_DASH_ENDPOINT
        self._batchuploader = None

    def get_job(self, *args, **kwargs):
        return Job(self, *args, **kwargs)

    def push_job(self, *args, **kwargs):
        raise AttributeError(
            "Scheduling jobs from client level is deprecated."
            "Please schedule new jobs via project.push_job()."
        )

    def project_ids(self):
        """Returns a list of available projects."""
        return self.projects.list()

# ------------------ special dash mixin class ------------------


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
        response.raise_for_status()
        data = response.json()
        try:
            if data['status'] == 'ok':
                return data
            elif data['status'] in ('error', 'badrequest'):
                raise ScrapinghubAPIError(data['message'])
            raise ScrapinghubAPIError(
                "Unknown response status: {0[status]}".format(data))
        except KeyError:
            raise ScrapinghubAPIError("JSON response does not contain status")


# ------------------ resource type classes section ------------------

class ResourceType(resourcetype.ResourceType, DashMixin):

    def __init__(self, client, key, auth=None):
        super(ResourceType, self).__init__(client, key, auth=auth)
        self._key = key


class MappingResourceType(resourcetype.MappingResourceType, ResourceType):
    """Custom MappingResourceType based on modified ResourceType.
    MRO: 1) MappingResourceType
         2) hubstorage.resourcetype.MappingResourceType
         3) ResourceType
         4) hubstorage.resourcetype.ResourceType"""


class ItemsResourceType(resourcetype.ItemsResourceType, ResourceType):
    """Custom ItemsResourceType based on modified ResourceType
    MRO: 1) ItemsResourceType
         2) hubstorage.resourcetype.ItemsResourceType
         3) ResourceType
         4) hubstorage.resourcetype.ResourceType"""


# ------------------ project classes section ---------------------


class Projects(_Projects, ResourceType):

    def get(self, *args, **kwargs):
        return Project(self.client, *args, **kwargs)

    def list(self):
        return self._dash_apiget(self.client.dash_endpoint,
                                 'scrapyd/listprojects.json')


class Project(_Project):

    def __init__(self, client, projectid, auth=None):
        self.client = client
        self.projectid = urlpathjoin(projectid)
        assert len(self.projectid.split('/')) == 1, \
            'projectkey must be just one id: %s' % projectid
        self.auth = xauth(auth) or client.auth
        self.jobs = Jobs(client, self.projectid, auth=auth)
        self.items = Items(client, self.projectid, auth=auth)
        self.logs = Logs(client, self.projectid, auth=auth)
        self.samples = ProjectSamples(client, self.projectid, auth=auth)
        self.jobq = JobQ(client, self.projectid, auth=auth)
        self.activity = Activity(client, self.projectid, auth=auth)
        self.collections = Collections(client, self.projectid, auth=auth)
        self.frontier = Frontier(client, self.projectid, auth=auth)
        self.ids = Ids(client, self.projectid, auth=auth)
        self.settings = Settings(client, self.projectid, auth=auth)
        self.reports = Reports(client, self.projectid, auth=auth)
        self.spiders = Spiders(client, self.projectid, auth=auth)

    def push_job(self, spidername, **jobparams):
        data = self.jobq.push(spidername, **jobparams)
        key = data['key']
        return Job(self.client, key, auth=self.auth)

    def count(self, **params):
        return self.jobq.count(**params)


class Spiders(_Spiders, ResourceType):

    def list(self):
        return self._dash_apiget(self.client.dash_endpoint,
                                 'spiders/list.json',
                                 params={'project': self._key})['spiders']

    def update_tags(self, spidername, add=None, remove=None):
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params.update({'spider': "noop", 'project': self._key})
        return self._dash_apipost(self.client.dash_endpoint,
                                  'jobs/update.json', data=params)['count']


# ------------------------ job-related section -----------------------

class Job(_Job):

    def __init__(self, client, key, auth=None, jobauth=None, metadata=None):
        self.key = urlpathjoin(key)
        assert len(self.key.split('/')) == 3, \
            'Jobkey must be projectid/spiderid/jobid: %s' % self.key
        self.jobauth = jobauth
        self.auth = self.jobauth or auth
        self.metadata = JobMeta(client, self.key, self.auth, cached=metadata)
        self.items = Items(client, self.key, self.auth)
        self.logs = Logs(client, self.key, self.auth)
        self.samples = JobSamples(client, self.key, self.auth)
        self.requests = Requests(client, self.key, self.auth)
        self.jobq = JobQ(client, self.key.split('/')[0], auth)

    def update_tags(self, *args, **kwargs):
        return self.metadata.update_tags(*args, **kwargs)


class JobMeta(_JobMeta, MappingResourceType):

    def update_tags(self, add=None, remove=None):
        params = get_tags_for_update(add_tag=add, remove_tag=remove)
        if not params:
            return
        params['job'] = self._key
        params['project'] = self._key.split('/', 1)[0]
        response = self._dash_apipost(self.client.dash_endpoint,
                                      'jobs/update.json', data=params)
        return response['count']


class Items(_Items, ItemsResourceType):

    def list(self, _key=None, **params):
        if 'offset' in params:
            params['start'] = '%s/%s' % (self._key, params['offset'])
            del params['offset']
        return self.apiget(_key, params=params)


class Logs(_Logs, ItemsResourceType):

    def list(self, _key=None, **params):
        if 'offset' in params:
            params['start'] = '%s/%s' % (self._key, params['offset'])
            del params['offset']
        if 'level' in params:
            minlevel = getattr(Log, params.get('level'), None)
            if minlevel is None:
                raise ScrapinghubAPIError(
                    "Unknown log level: %s" % params.get('level'))
            params['filters'] = ['level', '>=', [minlevel]]
        return self.apiget(_key, params=params)


# ------------------------ jobq section -----------------------


class JobQ(_JobQ, ResourceType):

    def count(self, **params):
        return next(self.apiget(('count',), params=params))

    def push(self, spider, **jobparams):
        jobparams['spider'] = spider
        # for client-level JobQ, project should be provided via jobparams
        if 'project' not in jobparams:
            if not self._key:
                raise ScrapinghubAPIError(
                    "Project is required when scheduling new jobs")
            jobparams['project'] = self._key
        # FIXME JobQ endpoint can schedule multiple jobs with json-lines,
        # corresponding Dash endpoint - only one job per request
        response = self._dash_apipost(self.client.dash_endpoint,
                                      'schedule.json',
                                      data=jobparams)
        if response.get('status') == 'error':
            if 'already scheduled' in response['message']:
                raise DuplicateJobError(response['message'])
            raise ScrapinghubAPIError(response['message'])
        return {'key': response.get('jobid'), 'auth': None}


# ------------------------ auxiliaries section -----------------------

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
