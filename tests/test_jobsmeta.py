"""
Test job metadata

System tests for operations on stored job metadata
"""
from .hstestcase import HSTestCase


class JobsMetadataTest(HSTestCase):

    def _assertMetadata(self, meta1, meta2):
        def _clean(m):
            return dict((k, v) for k, v in m.items() if k != 'updated_time')

        meta1 = _clean(meta1)
        meta2 = _clean(meta2)
        self.assertEqual(meta1, meta2)

    def test_basic(self):
        job = self.project.push_job(self.spidername)
        self.assertTrue('auth' not in job.metadata)
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

    def test_updating(self):
        job = self.project.push_job(self.spidername)
        self.assertIsNone(job.metadata.get('foo'))
        job.update_metadata({'foo': 'bar'})
        # metadata attr should change
        self.assertEqual(job.metadata.get('foo'), 'bar')
        # as well as actual metadata
        job = self.project.get_job(job.key)
        self.assertEqual(job.metadata.get('foo'), 'bar')
        job.update_metadata({'foo': None})
        self.assertFalse(job.metadata.get('foo', False))

        # there are ignored fields like: auth, _key, state
        state = job.metadata['state']
        job.update_metadata({'state': 'running'})
        self.assertEqual(job.metadata['state'], state)

    def test_representation(self):
        job = self.project.push_job(self.spidername)
        meta = job.metadata
        self.assertNotEqual(str(meta), repr(meta))
        self.assertEqual(meta, eval(str(meta)))
        self.assertTrue(meta.__class__.__name__ in repr(meta))
        self.assertFalse(meta.__class__.__name__ in str(meta))

    def test_jobauth(self):
        job = self.project.push_job(self.spidername)
        self.assertIsNone(job.jobauth)
        self.assertEqual(job.auth, self.project.auth)
        self.assertEqual(job.items.auth, self.project.auth)

        samejob = self.hsclient.get_job(job.key)
        self.assertIsNone(samejob.auth)
        self.assertIsNone(samejob.jobauth)
        self.assertEqual(samejob.items.auth, self.project.auth)

    def test_authtoken(self):
        pendingjob = self.project.push_job(self.spidername)
        runningjob = self.start_job()
        self.assertEqual(pendingjob.key, runningjob.key)
        self.assertTrue(runningjob.jobauth)
        self.assertEqual(runningjob.jobauth, runningjob.auth)
        self.assertEqual(runningjob.auth[0], runningjob.key)
        self.assertTrue(runningjob.auth[1])
