import os, unittest
from hubstorage import HSClient


class HSTestCase(unittest.TestCase):

    projectid = '1111111'
    endpoint = os.getenv('HS_ENDPOINT', 'http://localhost:8003')
    auth = os.getenv('HS_AUTH', 'useavalidkey')

    def setUp(self):
        self.hsclient = HSClient(auth=self.auth, endpoint=self.endpoint)
