import random
from six.moves import range
from contextlib import closing
from .hstestcase import HSTestCase
from hubstorage import HubstorageClient
from hubstorage.utils import millitime


class SystemTest(HSTestCase):

    MAGICN = 1211

    def setUp(self):
        super(HSTestCase, self).setUp()
        self.endpoint = self.hsclient.endpoint
        # Panel - no client auth, only project auth using user auth token
        self.panelclient = HubstorageClient(endpoint=self.endpoint)
        self.panelproject = self.panelclient.get_project(self.projectid, auth=self.auth)

    def tearDown(self):
        super(HSTestCase, self).tearDown()
        self.panelclient.close()

    def test_succeed_with_close_reason(self):
        self._do_test_success('all-good', 'all-good')

    def test_succeed_without_close_reason(self):
        self._do_test_success(None, 'no_reason')

    def test_scraper_failure(self):
        job = self._do_test_job(IOError('no more resources, ha!'), 'failed')
        # MAGICN per log level messages plus one of last failure
        stats = job.logs.stats()
        self.assertTrue(stats)
        self.assertEqual(stats['totals']['input_values'], self.MAGICN * 4 + 1)

    def _do_test_success(self, job_close_reason, expected_close_reason):
        job = self._do_test_job(job_close_reason, expected_close_reason)
        self.assertEqual(job.items.stats()['totals']['input_values'], self.MAGICN)
        self.assertEqual(job.logs.stats()['totals']['input_values'], self.MAGICN * 4)
        self.assertEqual(job.requests.stats()['totals']['input_values'], self.MAGICN)

    def _do_test_job(self, job_close_reason, expected_close_reason):
        p = self.panelproject
        pushed = p.jobq.push(self.spidername)
        # check pending state
        job = p.get_job(pushed['key'])
        self.assertEqual(job.metadata.get('state'), 'pending')
        # consume msg from runner
        self._run_runner(pushed, close_reason=job_close_reason)
        # query again from panel
        job = p.get_job(pushed['key'])
        self.assertEqual(job.metadata.get('state'), 'finished')
        self.assertEqual(job.metadata.get('close_reason'), expected_close_reason)
        return job

    def _run_runner(self, pushed, close_reason):
        client = HubstorageClient(endpoint=self.endpoint, auth=self.auth)
        with closing(client) as runnerclient:
            job = self.start_job()
            self.assertFalse(job.metadata.get('stop_requested'))
            job.metadata.update(host='localhost', slot=1)
            self.assertEqual(job.metadata.get('state'), 'running')
            # run scraper
            try:
                self._run_scraper(job.key, job.jobauth, close_reason=close_reason)
            except Exception as exc:
                job.logs.error(message=str(exc), appendmode=True)
                job.close_writers()
                job.jobq.finish(job, close_reason='failed')
                # logging from runner must append and never remove messages logged
                # by scraper
                self.assertTrue(job.logs.batch_append)
            else:
                job.jobq.finish(job, close_reason=close_reason or 'no_reason')

    def _run_scraper(self, jobkey, jobauth, close_reason=None):
        httpmethods = 'GET PUT POST DELETE HEAD OPTIONS TRACE CONNECT'.split()
        # Scraper - uses job level auth, no global or project auth available
        client = HubstorageClient(endpoint=self.endpoint)
        with closing(client) as scraperclient:
            job = scraperclient.get_job(jobkey, auth=jobauth)
            for idx in range(self.MAGICN):
                iid = job.items.write({'uuid': idx})
                job.logs.debug('log debug %s' % idx, idx=idx)
                job.logs.info('log info %s' % idx, idx=idx)
                job.logs.warn('log warn %s' % idx, idx=idx)
                job.logs.error('log error %s' % idx, idx=idx)
                sid = job.samples.write([idx, idx, idx])
                rid = job.requests.add(
                    url='http://test.com/%d' % idx,
                    status=random.randint(100, 1000),
                    method=random.choice(httpmethods),
                    rs=random.randint(0, 100000),
                    duration=random.randint(0, 1000),
                    parent=random.randrange(0, idx + 1) if idx > 10 else None,
                    ts=millitime() + random.randint(100, 100000),
                )
                self.assertEqual(iid, idx)
                self.assertEqual(sid, idx)
                self.assertEqual(rid, idx)

            if isinstance(close_reason, Exception):
                raise close_reason

            if close_reason:
                job.metadata['close_reason'] = close_reason

            job.metadata.save()
