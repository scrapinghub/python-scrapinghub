"""
High level Hubstorage client
"""
import os
import pkgutil
from requests import session
from .utils import xauth
from .project import Project
from .job import Job
from .batchuploader import BatchUploader

__all__ = ["HubstorageClient"]
__version__ = pkgutil.get_data('hubstorage', 'VERSION').strip()


class HubstorageClient(object):

    DEFAULT_ENDPOINT = 'http://storage.scrapinghub.com:8002'
    USERAGENT = 'python-hubstorage/{0}'.format(__version__)

    def __init__(self, auth=None, endpoint=None):
        self.auth = xauth(auth)
        self.endpoint = endpoint or os.environ.get('SHUB_STORAGE') or \
                self.DEFAULT_ENDPOINT
        self.session = self._create_session()
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

    def new_job(self, projectid, spidername, auth=None, **jobparams):
        project = self.get_project(projectid, auth=auth)
        return project.new_job(spidername, **jobparams)

    def get_project(self, *args, **kwargs):
        return Project(self, *args, **kwargs)

    def close(self, timeout=None):
        if self._batchuploader is not None:
            self.batchuploader.close(timeout)
