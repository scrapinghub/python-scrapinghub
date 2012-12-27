import os, unittest
from hubstorage import HubstorageClient


class HSTestCase(unittest.TestCase):

    projectid = '1111111'
    spidername = 'spidey'
    endpoint = os.getenv('HS_ENDPOINT', 'http://localhost:8003')
    auth = os.getenv('HS_AUTH', 'useavalidkey')

    @classmethod
    def setUpClass(cls):
        cls.hsclient = HubstorageClient(auth=cls.auth, endpoint=cls.endpoint)
        cls.project = cls.hsclient.get_project(cls.projectid)
        cls.testjob = cls.project.new_job(cls.spidername)

    @classmethod
    def tearDownClass(cls):
        cls.testjob.purged()
        prefix, _, _ = cls.testjob.key.rpartition('/')
        for jd in cls.project.jobq.summary('pending')['summary']:
            key = jd['key']
            if key.startswith(prefix):
                cls.hsclient.get_job(key).purged()

        cls.hsclient.close()
