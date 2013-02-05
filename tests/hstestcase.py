import os
import unittest
from hubstorage import HubstorageClient


class HSTestCase(unittest.TestCase):

    projectid = '1111111'
    spidername = 'hs-test-spider'
    endpoint = os.getenv('HS_ENDPOINT', 'http://localhost:8003')
    auth = os.getenv('HS_AUTH', 'useavalidkey')

    @classmethod
    def setUpClass(cls):
        cls.hsclient = HubstorageClient(auth=cls.auth, endpoint=cls.endpoint)
        cls.project = cls.hsclient.get_project(cls.projectid)
        cls.spiderid = str(cls.project.ids.spider(cls.spidername, create=1))

    def setUp(self):
        self._remove_all_jobs()

    def tearDown(self):
        self._remove_all_jobs()

    @classmethod
    def tearDownClass(cls):
        cls._remove_all_jobs()

    @classmethod
    def _remove_all_jobs(cls):
        jobq = cls.project.jobq
        # Cleanup JobQ
        for queuename in ('pending', 'running', 'finished'):
            info = {'summary': [None]}  # poor-guy do...while
            while info['summary']:
                info = jobq.summary(queuename, spiderid=cls.spiderid)
                for summary in info['summary']:
                    cls._remove_job(summary['key'])

        # Cleanup jobs created directly with jobsmeta instead of jobq
        for job in cls.project.get_jobs(cls.spiderid):
            cls._remove_job(job.key)

        cls.hsclient.close()

    @classmethod
    def _remove_job(cls, jobkey):
        validprefix = '%s/%s' % (cls.projectid, cls.spiderid)
        assert jobkey.startswith(validprefix), jobkey
        cls.project.jobq.delete(jobkey)
        cls.project.jobs.apidelete(jobkey.partition('/')[2])


class NopTest(HSTestCase):

    def test_nop(self):
        pass  # hooray!


if __name__ == '__main__':
    unittest.main()
