"""
High level Hubstorage client
"""
import os, requests
from .utils import xauth
from .jobs import Jobs
from .jobq import JobQ
from .activity import Activity
from .collectionsrt import Collections


ENDPOINT = os.environ.get('SHUB_STORAGE', 'http://storage.scrapinghub.com:8002')


class HSClient(object):

    def __init__(self, auth=None, endpoint=ENDPOINT):
        self.auth = xauth(auth)
        self.endpoint = endpoint
        self.conn = requests.session()

    def get_job(self, key, auth=None):
        auth = xauth(auth) or self.auth
        return Jobs(key, client=self, auth=auth)

    def new_job(self, projectid, spider, **jobparams):
        jobq = self.get_jobq(projectid)
        data = jobq.push(spider, **jobparams)
        key = data['key']
        auth = (key, data['auth'])
        return Jobs(key, client=self, auth=auth)

    def get_jobq(self, projectid):
        return JobQ(projectid, client=self, auth=self.auth)

    def get_activity(self, projectid):
        return Activity(projectid, client=self, auth=self.auth)

    def get_collections(self, projectid):
        return Collections(projectid, client=self, auth=self.auth)
