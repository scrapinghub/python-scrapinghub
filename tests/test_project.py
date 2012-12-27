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
        self.assertRaises(AssertionError, self.hsclient.get_project, '111/3')

    def test_job(self):
        job = self.project.new_job('spidey')
        parts = tuple(job.key.split('/'))
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], self.projectid)
        samejob = self.project.get_job(parts[1:])
        samejob2 = self.hsclient.get_job(job.key)
        self.assertEqual(samejob.key, job.key)
        self.assertEqual(samejob2.key, job.key)

    def test_auth(self):
        # client without global auth set
        hsc = HubstorageClient(endpoint=self.hsclient.endpoint)
        self.assertEqual(hsc.auth, None)

        # check no-auth access
        try:
            hsc.new_job(self.projectid, 'spidey')
        except HTTPError as exc:
            self.assertTrue(exc.response.status_code, 401)

        try:
            hsc.get_project(self.projectid).new_job('spidey')
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
        job = project.new_job('spidey')
        samejob = project.get_job(job.key)
        self.assertTrue(samejob.key, job.key)
        samejob.purged()

    def test_broad(self):
        project = self.hsclient.get_project(self.projectid)
        # populate project with at least one job
        job = project.new_job('spidey')
        self.assertEqual(job.metadata.get('state'), 'pending')
        job.started()
        self.assertEqual(job.metadata.get('state'), 'running')
        job.items.write({'title': 'bar'})
        job.logs.info('nice to meet you')
        job.samples.write([1, 2, 3])
        job.finished()
        job.samples.flush()
        job.items.flush()
        job.logs.flush()

        # keep a jobid for get_job and unreference job
        jobid = job.key
        jobauth = job.auth
        del job

        spiderid = jobid.split('/')[1]
        self.assertTrue(list(project.jobs.list(spiderid, count=1, meta='_key')))
        self.assertTrue(list(project.items.list(spiderid, count=1, meta='_key')))
        self.assertTrue(list(project.logs.list(spiderid, count=1, meta='_key')))
        self.assertTrue(list(project.samples.list(spiderid, count=1, meta='_key')))

        job = project.client.get_job(jobid, auth=jobauth)
        job.purged()
