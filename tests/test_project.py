"""
Test Project
"""
from hstestcase import HSTestCase
from hubstorage import HubstorageClient
from requests.exceptions import HTTPError


class ProjectTest(HSTestCase):

    def test_projectid(self):
        p1 = self.hsclient.get_project(int(self.projectid))
        p2 = self.hsclient.get_project(str(self.projectid))
        self.assertEqual(p1.projectid, p2.projectid)
        self.assertEqual(type(p1.projectid), str)
        self.assertEqual(type(p2.projectid), str)
        self.assertRaises(AssertionError, self.hsclient.get_project, '111/3')

    def test_get_job_from_key(self):
        job = self.project.new_job(self.spidername)
        parts = tuple(job.key.split('/'))
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[:2], (self.projectid, self.spiderid))
        samejob1 = self.hsclient.get_job(job.key)
        samejob2 = self.project.get_job(job.key)
        samejob3 = self.project.get_job(parts[1:])
        self.assertEqual(samejob1.key, job.key)
        self.assertEqual(samejob2.key, job.key)
        self.assertEqual(samejob3.key, job.key)

    def test_get_jobs(self):
        p = self.project
        j1 = p.new_job(self.spidername, testid=0)
        j2 = p.new_job(self.spidername, testid=1)
        j3 = p.new_job(self.spidername, testid=2)
        # global list must list at least one job
        self.assertTrue(list(p.get_jobs(count=1)))
        # List all jobs for test spider
        r = list(p.get_jobs(self.spiderid))
        self.assertEqual([j.key for j in r], [j1.key, j2.key, j3.key])

    def test_auth(self):
        # client without global auth set
        hsc = HubstorageClient(endpoint=self.hsclient.endpoint)
        self.assertEqual(hsc.auth, None)

        # check no-auth access
        try:
            hsc.new_job(self.projectid, self.spidername)
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)

        try:
            hsc.get_project(self.projectid).new_job(self.spidername)
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)

        try:
            hsc.get_job((self.projectid, 1, 1))
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)

        try:
            hsc.get_project(self.projectid).get_job((self.projectid, 1, 1))
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)

        # create project with auth
        auth = self.hsclient.auth
        project = hsc.get_project(self.projectid, auth)
        self.assertEqual(project.auth, auth)
        job = project.new_job(self.spidername)
        samejob = project.get_job(job.key)
        self.assertEqual(samejob.key, job.key)

    def test_broad(self):
        project = self.hsclient.get_project(self.projectid)
        # populate project with at least one job
        job = project.new_job(self.spidername)
        self.assertEqual(job.metadata.get('state'), 'pending')
        job.started()
        self.assertEqual(job.metadata.get('state'), 'running')
        job.items.write({'title': 'bar'})
        job.logs.info('nice to meet you')
        job.samples.write([1, 2, 3])
        job.finished()

        # keep a jobid for get_job and unreference job
        jobid = job.key
        jobauth = job.auth
        del job

        self.assertTrue(list(project.jobs.list(self.spiderid, count=1)))
        self.assertTrue(list(project.items.list(self.spiderid, count=1)))
        self.assertTrue(list(project.logs.list(self.spiderid, count=1)))
        self.assertTrue(list(project.samples.list(self.spiderid, count=1)))

        job = project.client.get_job(jobid, jobauth=jobauth)
        job.purged()
