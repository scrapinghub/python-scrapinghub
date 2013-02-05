from hstestcase import HSTestCase
from hubstorage import HubstorageClient


class SystemTest(HSTestCase):

    MAGICN = 1211

    def setUp(self):
        super(HSTestCase, self).setUp()
        endpoint = self.hsclient.endpoint
        # Panel - no client auth, only project auth using user auth token
        self.panelclient = HubstorageClient(endpoint=endpoint)
        self.panelproject = self.panelclient.get_project(self.projectid, auth=self.auth)
        # Runner - client uses global auth to poll jobq
        self.runnerclient = HubstorageClient(endpoint=endpoint, auth=self.auth)
        # Scraper - uses job level auth, no global or project auth available
        self.scraperclient = HubstorageClient(endpoint=endpoint)

    def test_succeed_with_close_reason(self):
        p = self.panelproject
        pushed = p.jobq.push(self.spidername)
        # check pending state
        job = p.get_jobs(self.spiderid).next()
        self.assertEqual(job.metadata.get('state'), 'pending')
        # consume msg from runner
        self._run_runner(pushed, close_reason='all-good')
        # query again from panel
        job = p.get_jobs(self.spiderid).next()
        self.assertEqual(job.metadata.get('state'), 'finished')
        self.assertEqual(job.metadata.get('close_reason'), 'all-good')

    def test_succeed_without_close_reason(self):
        p = self.panelproject
        pushed = p.jobq.push(self.spidername)
        # check pending state
        job = p.get_jobs(self.spiderid).next()
        self.assertEqual(job.metadata.get('state'), 'pending')
        # consume msg from runner
        self._run_runner(pushed, close_reason=None)
        # query again from panel
        job = p.get_jobs(self.spiderid).next()
        self.assertEqual(job.metadata.get('state'), 'finished')
        self.assertEqual(job.metadata.get('close_reason'), 'no_reason')

    def test_scraper_failure(self):
        p = self.panelproject
        pushed = p.jobq.push(self.spidername)
        # check pending state
        job = p.get_jobs(self.spiderid).next()
        self.assertEqual(job.metadata.get('state'), 'pending')
        # consume msg from runner
        self._run_runner(pushed, close_reason=IOError('no more resources, ha!'))
        # query again from panel
        job = p.get_jobs(self.spiderid).next()
        self.assertEqual(job.metadata.get('state'), 'finished')
        self.assertEqual(job.metadata.get('close_reason'), 'failed')
        # MAGICN per log level messages plus one of last failure
        stats = job.logs.stats()
        self.assertTrue(stats)
        self.assertEqual(stats['totals']['input_values'], self.MAGICN * 4 + 1)

    def _run_runner(self, pushed, close_reason):
        job = self.runnerclient.start_job(self.projectid)
        self.assertFalse(job.metadata.get('stop_requested'))
        job.metadata.update(host='localhost', slot=1)
        self.assertEqual(job.metadata.get('state'), 'running')
        # run scraper
        try:
            self._run_scraper(job.key, job.jobauth, close_reason=close_reason)
        except Exception as exc:
            job.failed(message=str(exc))
            # logging from runner must append and never remove messages logged
            # by scraper
            self.assertTrue(job.logs.batch_append)
        else:
            job.finished()

        self.runnerclient.close()

    def _run_scraper(self, jobkey, jobauth, close_reason=None):
        job = self.scraperclient.get_job(jobkey, auth=jobauth)
        for idx in xrange(self.MAGICN):
            job.items.write({'uuid': idx})
            job.logs.debug('log debug %s' % idx, idx=idx)
            job.logs.info('log info %s' % idx, idx=idx)
            job.logs.warn('log warn %s' % idx, idx=idx)
            job.logs.error('log error %s' % idx, idx=idx)
            job.samples.write([idx, idx, idx])

        if isinstance(close_reason, Exception):
            self.scraperclient.close()
            raise close_reason

        if close_reason:
            job.metadata['close_reason'] = close_reason

        job.metadata.save()
        self.scraperclient.close()
        del self.scraperclient
