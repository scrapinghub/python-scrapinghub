"""
Test Project
"""
from hstestcase import HSTestCase


class ProjectTest(HSTestCase):
    def test_get_project_from_int_id(self):
        projectid = int(self.projectid)
        project = self.hsclient.get_project(projectid)
        self.assertEqual(project.projectid, projectid)

    def test_get_project_from_str_id(self):
        projectid = str(self.projectid)
        project = self.hsclient.get_project(projectid)
        self.assertEqual(project.projectid, projectid)
