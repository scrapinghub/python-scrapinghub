"""
High level Hubstorage client
"""
import pkgutil
from requests import session, adapters
from .utils import xauth, urlpathjoin
from .project import Project
from .job import Job
from .jobq import JobQ
from .batchuploader import BatchUploader
from .resourcetype import ResourceType

__all__ = ["HubstorageClient"]
__version__ = pkgutil.get_data('hubstorage', 'VERSION').strip()


class HubstorageClient(object):

    DEFAULT_ENDPOINT = 'http://storage.scrapinghub.com/'
    USERAGENT = 'python-hubstorage/{0}'.format(__version__)
    DEFAULT_TIMEOUT = 60.0

    def __init__(self, auth=None, endpoint=None, connection_timeout=None,
            max_retries=0):
        self.auth = xauth(auth)
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.connection_timeout = connection_timeout or self.DEFAULT_TIMEOUT
        self.session = self._create_session(max_retries)
        self.jobq = JobQ(self, None)
        self.projects = Projects(self, None)
        self.root = ResourceType(self, None)
        self._batchuploader = None

    def _create_session(self, max_retries):
        s = session()
        if max_retries > 0:
            a = adapters.HTTPAdapter(max_retries=max_retries)
            s.mount('http://', a)
            s.mount('https://', a)
        s.headers.update({'User-Agent': self.USERAGENT})
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

    def start_job(self, projectid=None, auth=None, **startparams):
        """Start a new job

        It may take up to a second for a previously added job to be available
        here if no project id is specified.

        """
        if projectid:
            jobq = self.projects.get(projectid, auth=auth).jobq
        else:
            jobq = self.jobq
        jobdata = jobq.start(**startparams)
        if jobdata:
            jobkey = jobdata.pop('key')
            jobauth = (jobkey, jobdata['auth'])
            return self.get_job(jobkey, jobauth=jobauth, metadata=jobdata)

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
