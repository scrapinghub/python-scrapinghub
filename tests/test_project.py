"""
Test Project
"""
from hstestcase import HSTestCase


class ProjectTest(HSTestCase):

    def test_projectid(self):
        p1 = self.hsclient.get_project(int(self.projectid))
        p2 = self.hsclient.get_project(str(self.projectid))
        self.assertEqual(p1.projectid, p2.projectid)
        self.assertEqual(type(p1.projectid), str)
        self.assertRaises(AssertionError, self.hsclient.get_project, '111/3')

    def test_jobs(self):
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
