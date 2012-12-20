"""
Test job metadata

System tests for operations on stored job metadata
"""
import random
from hstestcase import HSTestCase
import logging


def _clean(data):
    for k in ('_key', 'auth', 'updated_time'):
        data.pop(k, None)
    return data


class JobsMetaTest(HSTestCase):

    spider_increment = random.randint(10, 1000)
    meta1 = {'spider': 'groupon', 'spider_type': 'manual'}
    meta2 = {'spider': 'google', 'spider_type': 'manual', 'started_time': 1309957061995}

    @classmethod
    def setUpClass(cls):
        HSTestCase.setUpClass()
        cls.job1 = cls.project.get_job((cls.spider_increment, 1))
        cls.job1.update(cls.meta1)
        cls.job2 = cls.project.get_job((cls.spider_increment + 1, 1))
        cls.job2.update(cls.meta2)

    @classmethod
    def tearDownClass(cls):
        for i in xrange(3):
            for jobinfo in cls.project.jobs.get(cls.spider_increment + i, meta='_key'):
                logging.info('%s', jobinfo)
                jobkey = jobinfo.pop('_key')
                job = cls.hsclient.get_job(jobkey)
                for key in jobinfo:
                    job.jobsmeta.delete(key)

        HSTestCase.tearDownClass()

    def test_get(self):
        self.assertEqual(_clean(self.job1.metadata), self.meta1)
        self.assertEqual(self.job1.metadata['spider'], 'groupon')
        self.job1.expire()
        self.assertEqual(self.job1.metadata['spider'], 'groupon')

    def test_list(self):
        items = []
        for job in self.project.get_jobs():
            projectid, spiderid, jobid = job.key.split('/')
            self.assertEqual(projectid, self.projectid)
            if int(spiderid) in (self.spider_increment, self.spider_increment + 1):
                items.append(_clean(job.metadata))

        self.assertEqual(len(items), 2, items)
        self.assertEqual(items[0], self.meta1)
        self.assertEqual(items[1], self.meta2)

    def test_post_delete(self):
        self.job1.update(foo='bar')
        self.job1.expire()
        refmeta = dict(self.meta1, foo='bar')
        self.assertEqual(_clean(self.job1.metadata), refmeta)

        self.job1.jobsmeta.delete('foo')
        self.job1.expire()
        self.assertEqual(_clean(self.job1.metadata), self.meta1)

    def test_authtoken_setting(self):
        token = self.job1.jobsmeta.get('auth').next()
        self.assertEqual(len(token), 8)
        token2 = self.job1.jobsmeta.get('auth').next()
        self.assertEqual(token, token2)

    def test_purge(self):
        jobid = str(random.randint(1, 1000000))
        job = self.project.get_job((self.spider_increment + 2, jobid))
        # add samples
        job.items.write({'foo': 'bar'})
        job.logs.debug(message='hello')
        job.logs.info(message='hello again')
        job.items.flush()
        job.logs.flush()
        self.assertEqual(len(list(job.items.get())), 1)
        self.assertEqual(len(list(job.logs.get())), 2)
        # purge job and check its items/logs are removed
        job.purged()
        self.assertEqual(len(list(job.items.get())), 0)
        self.assertEqual(len(list(job.logs.get())), 0)
