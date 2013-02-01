"""
High level Hubstorage client
"""
import pkgutil
from requests import session
from .utils import xauth
from .project import Project
from .job import Job
from .jobq import JobQ
from .batchuploader import BatchUploader

__all__ = ["HubstorageClient"]
__version__ = pkgutil.get_data('hubstorage', 'VERSION').strip()


class HubstorageClient(object):

    DEFAULT_ENDPOINT = 'http://storage.scrapinghub.com:8002'
    USERAGENT = 'python-hubstorage/{0}'.format(__version__)

    def __init__(self, auth=None, endpoint=None):
        self.auth = xauth(auth)
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.session = self._create_session()
        self._batchuploader = None
        self.jobq = JobQ(self, None)

    def _create_session(self):
        s = session()
        s.headers.update({'User-Agent': self.USERAGENT})
        return s

    @property
    def batchuploader(self):
        if self._batchuploader is None:
            self._batchuploader = BatchUploader(self)
        return self._batchuploader

    def get_job(self, *args, **kwargs):
        return Job(self, *args, **kwargs)

    def new_job(self, projectid, spidername, auth=None, **jobparams):
        project = self.get_project(projectid, auth=auth)
        return project.new_job(spidername, **jobparams)

    def next_job(self, projectid, auth=None):
        # XXX: jobq is restricted to projects at the moment
        # but this will change
        project = self.get_project(projectid, auth=auth)
        data = project.jobq.poll()
        if data:
            jobkey = data['key']
            jobauth = (jobkey, data['auth'])
            return project.get_job(jobkey, jobauth=jobauth)

    def get_project(self, *args, **kwargs):
        return Project(self, *args, **kwargs)

    def close(self, timeout=None):
        if self._batchuploader is not None:
            self.batchuploader.close(timeout)
