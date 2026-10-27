import mock

from scrapinghub.client.jobs import Jobs


def _mock_jobs(total):
    def list_(start=0, count=None, **params):
        end = total if count is None else min(total, start + count)
        return iter(range(start, end))

    jobs = Jobs(mock.Mock(), '1')
    jobs._project.jobq.list.side_effect = list_
    return jobs


def test_iter_paginates():
    jobs = _mock_jobs(2500)
    assert list(jobs.iter()) == list(range(2500))
    assert list(jobs.iter(start=500, count=1200)) == list(range(500, 1700))
    assert list(jobs.iter(count=3)) == [0, 1, 2]
    assert list(_mock_jobs(1000).iter()) == list(range(1000))
