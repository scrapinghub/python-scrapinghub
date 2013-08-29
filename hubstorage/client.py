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

    DEFAULT_ENDPOINT = 'http://storage.scrapinghub.com/'
    USERAGENT = 'python-hubstorage/{0}'.format(__version__)
    DEFAULT_TIMEOUT = 60.0

    def __init__(self, auth=None, endpoint=None, connection_timeout=None):
        self.auth = xauth(auth)
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.connection_timeout = connection_timeout or self.DEFAULT_TIMEOUT
        self.session = self._create_session()
        self.jobq = JobQ(self, None, auth=self.auth)
        self._batchuploader = None

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

    def push_job(self, projectid, spidername, auth=None, **jobparams):
        project = self.get_project(projectid, auth=auth)
        return project.push_job(spidername, **jobparams)

    def start_job(self, projectid=None, auth=None, **startparams):
        if projectid:
            jobq = self.get_project(projectid, auth=auth).jobq
        else:
            jobq = self.jobq

        jobdata = jobq.start(**startparams)
        if jobdata:
            jobkey = jobdata.pop('key')
            jobauth = (jobkey, jobdata['auth'])
            return self.get_job(jobkey, jobauth=jobauth, metadata=jobdata)

    def get_project(self, *args, **kwargs):
        return Project(self, *args, **kwargs)


    def close(self, timeout=None):
        if self._batchuploader is not None:
            self.batchuploader.close(timeout)
