"""
Test job metadata

System tests for operations on stored job metadata
"""
import random
from hstestcase import HSTestCase
import logging


def _clean(metadata):
    return dict((k, v) for k, v in metadata.items()
                if k not in ('_key', 'auth', 'updated_time'))


class JobsMetaTest(HSTestCase):

    spider_increment = random.randint(10, 1000)
    meta1 = {'spider': 'groupon', 'spider_type': 'manual'}
    meta2 = {'spider': 'google', 'spider_type': 'manual', 'started_time': 1309957061995}

    @classmethod
    def setUpClass(cls):
        HSTestCase.setUpClass()
        cls.job1 = cls.project.get_job((cls.spider_increment, 1))
        cls.job1.metadata.update(cls.meta1)
        cls.job1.metadata.save()
        cls.job2 = cls.project.get_job((cls.spider_increment + 1, 1))
        cls.job2.metadata.update(cls.meta2)
        cls.job2.metadata.save()

    @classmethod
    def tearDownClass(cls):
        for i in xrange(3):
            for jobinfo in cls.project.jobs.list(cls.spider_increment + i, meta='_key'):
                logging.info('%s', jobinfo)
                jobkey = jobinfo.pop('_key')
                job = cls.hsclient.get_job(jobkey)
                for key in jobinfo:
                    del job.metadata[key]
                job.metadata.save()

        HSTestCase.tearDownClass()

    def test_get(self):
        self.assertEqual(_clean(self.job1.metadata), self.meta1)
        self.assertEqual(self.job1.metadata.get('spider'), 'groupon')
        self.job1.metadata.expire()
        self.assertEqual(self.job1.metadata.get('spider'), 'groupon')

    def test_list(self):
        metas = []
        for job in self.project.get_jobs():
            projectid, spiderid, jobid = job.key.split('/')
            self.assertEqual(projectid, self.projectid)
            if int(spiderid) in (self.spider_increment, self.spider_increment + 1):
                metas.append(_clean(job.metadata))

        self.assertEqual(len(metas), 2, metas)
        self.assertEqual(metas[0], self.meta1)
        self.assertEqual(metas[1], self.meta2)

    def test_post_delete(self):
        self.job1.metadata['foo'] = 'bar'
        self.job1.metadata.save()
        self.job1.metadata.expire()
        refmeta = dict(self.meta1, foo='bar')
        self.assertEqual(_clean(self.job1.metadata), refmeta)
        del self.job1.metadata['foo']
        self.job1.metadata.save()
        self.job1.metadata.expire()
        self.assertEqual(_clean(self.job1.metadata), self.meta1)

    def test_authtoken_setting(self):
        token = self.job1.metadata.apiget('auth').next()
        self.assertEqual(len(token), 8)
        self.assertEqual(self.job1.auth, (self.job1.key, token))

    def test_purge(self):
        jobid = str(random.randint(1, 1000000))
        job = self.project.get_job((self.spider_increment + 2, jobid))
        # add samples
        job.items.write({'foo': 'bar'})
        job.logs.debug(message='hello')
        job.logs.info(message='hello again')
        job.items.flush()
        job.logs.flush()
        self.assertEqual(len(list(job.items.list())), 1)
        self.assertEqual(len(list(job.logs.list())), 2)
        # purge job and check its items/logs are removed
        job.purged()
        self.assertEqual(len(list(job.items.list())), 0)
        self.assertEqual(len(list(job.logs.list())), 0)
