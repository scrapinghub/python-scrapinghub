import os
import unittest
import random
import requests
from hubstorage import HubstorageClient
from hubstorage.utils import urlpathjoin


class HSTestCase(unittest.TestCase):

    projectid = str(random.randint(2222000, 2223000))
    spidername = 'hs-test-spider'
    endpoint = os.getenv('HS_ENDPOINT', 'http://storage.vm.scrapinghub.com')
    auth = os.getenv('HS_AUTH', 'f' * 32)
    frontier = 'test'
    slot = 'site.com'
    testbotgroups = ['python-hubstorage-test', 'g1']

    @classmethod
    def setUpClass(cls):
        cls.hsclient = HubstorageClient(auth=cls.auth, endpoint=cls.endpoint)
        cls.project = cls.hsclient.get_project(cls.projectid)
        cls.spiderid = str(cls.project.ids.spider(cls.spidername, create=1))
        cls._set_testbotgroup()

    def setUp(self):
        self._set_testbotgroup()
        self._remove_all_jobs()

    def tearDown(self):
        self._remove_all_jobs()

    @classmethod
    def tearDownClass(cls):
        cls.hsclient.close()
        cls._remove_all_jobs()
        cls._unset_testbotgroup()

    @classmethod
    def _remove_all_jobs(cls):
        project = cls.project
        for k in list(project.settings.keys()):
            if k != 'botgroups':
                del project.settings[k]
        project.settings.save()

        # Cleanup JobQ
        jobq = project.jobq
        for queuename in ('pending', 'running', 'finished'):
            info = {'summary': [None]}  # poor-guy do...while
            while info['summary']:
                info = jobq.summary(queuename)
                for summary in info['summary']:
                    cls._remove_job(summary['key'])

    @classmethod
    def _remove_job(cls, jobkey):
        cls.project.jobq.finish(jobkey)
        cls.project.jobq.delete(jobkey)
        cls._delete_job(jobkey)

    @classmethod
    def _delete_job(cls, jobkey):
        assert jobkey.startswith(cls.projectid), jobkey
        cls.project.jobs.apidelete(jobkey.partition('/')[2])

    @classmethod
    def _set_testbotgroup(cls):
        cls.project.settings.apipost(jl={'botgroups': [cls.testbotgroups[0]]})
        # Additional step to populate JobQ's botgroups table
        for botgroup in cls.testbotgroups:
            url = urlpathjoin(cls.endpoint, 'botgroups',
                              botgroup, 'max_running')
            requests.post(url, auth=cls.project.auth, data='null')
        cls.project.settings.expire()

    @classmethod
    def _unset_testbotgroup(cls):
        cls.project.settings.apidelete('botgroups')
        cls.project.settings.expire()
        # Additional step to delete botgroups in JobQ
        for botgroup in cls.testbotgroups:
            url = urlpathjoin(cls.endpoint, 'botgroups', botgroup)
            requests.delete(url, auth=cls.project.auth)

    def start_job(self, **startparams):
        jobdata = self.project.jobq.start(**startparams)
        if jobdata:
            jobkey = jobdata.pop('key')
            jobauth = (jobkey, jobdata['auth'])
            return self.project.get_job(jobkey, jobauth=jobauth, metadata=jobdata)


class NopTest(HSTestCase):

    def test_nop(self):
        pass  # hooray!


if __name__ == '__main__':
    unittest.main()
