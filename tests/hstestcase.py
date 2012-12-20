import os, unittest
from hubstorage import HubstorageClient


class HSTestCase(unittest.TestCase):

    projectid = '1111111'
    endpoint = os.getenv('HS_ENDPOINT', 'http://localhost:8003')
    auth = os.getenv('HS_AUTH', 'useavalidkey')

    @classmethod
    def setUpClass(cls):
        cls.hsclient = HubstorageClient(auth=cls.auth, endpoint=cls.endpoint)
        cls.project = cls.hsclient.get_project(cls.projectid)

    @classmethod
    def tearDownClass(cls):
        cls.hsclient.close()
