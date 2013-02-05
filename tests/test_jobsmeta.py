"""
Test job metadata

System tests for operations on stored job metadata
"""
from hstestcase import HSTestCase


class JobsMetadataTest(HSTestCase):

    def _assertMetadata(self, meta1, meta2):
        def _clean(m):
            return dict((k, v) for k, v in m.items() if k != 'updated_time')

        meta1 = _clean(meta1)
        meta2 = _clean(meta2)
        self.assertEqual(meta1, meta2)

    def test_basic(self):
        job = self.project.push_job(self.spidername)
        self.assertTrue('auth' in job.metadata)
        self.assertTrue('state' in job.metadata)
        self.assertEqual(job.metadata['spider'], self.spidername)

        # set some metadata and forget it
        job.metadata['foo'] = 'bar'
        self.assertEqual(job.metadata['foo'], 'bar')
        job.metadata.expire()
        self.assertTrue('foo' not in job.metadata)

        # set it again and persist it
        job.metadata['foo'] = 'bar'
        self.assertEqual(job.metadata['foo'], 'bar')
        job.metadata.save()
        self.assertEqual(job.metadata['foo'], 'bar')
        job.metadata.expire()
        self.assertEqual(job.metadata['foo'], 'bar')

        # refetch the job and compare its metadata
        job2 = self.hsclient.get_job(job.key)
        self._assertMetadata(job2.metadata, job.metadata)

        # delete foo but do not persist it
        del job.metadata['foo']
        self.assertTrue('foo' not in job.metadata)
        job.metadata.expire()
        self.assertEqual(job.metadata.get('foo'), 'bar')
        # persist it to be sure it is not removed
        job.metadata.save()
        self.assertEqual(job.metadata.get('foo'), 'bar')
        # and finally delete again and persist it
        del job.metadata['foo']
        self.assertTrue('foo' not in job.metadata)
        job.metadata.save()
        self.assertTrue('foo' not in job.metadata)
        job.metadata.expire()
        self.assertTrue('foo' not in job.metadata)

        job2 = self.hsclient.get_job(job.key)
        self._assertMetadata(job.metadata, job2.metadata)

    def test_jobauth(self):
        job = self.project.push_job(self.spidername)
        self.assertEqual(job.auth, job.jobauth)
        samejob = self.hsclient.get_job(job.key)
        self.assertEqual(samejob.jobauth, job.jobauth)
        self.assertEqual(samejob.items.auth, self.project.auth)

    def test_authtoken(self):
        job = self.project.push_job(self.spidername)
        job.metadata.expire()
        token = job.metadata.apiget('auth').next()
        self.assertEqual(job.jobauth, (job.key, token))
        self.assertEqual(job.metadata.get('auth'), token)
