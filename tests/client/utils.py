from ..conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from ..conftest import TEST_DASH_ENDPOINT


def validate_default_meta(meta, state='pending', units=1,
                          priority=2, tags=None):
    assert meta.get('project') == int(TEST_PROJECT_ID)
    assert meta.get('spider') == TEST_SPIDER_NAME
    assert meta.get('state') == state
    assert meta.get('priority') == priority
    assert meta.get('spider_type') == 'manual'
    assert meta.get('tags') == (tags or [])
    assert meta.get('units') == units
    assert meta.get('api_url') == TEST_DASH_ENDPOINT
    assert meta.get('portia_url')


def normalize_job_for_tests(job):
    """A temporary workaround to deal with VCR.py cassettes(snapshots).

    The existing tests highly rely on VCR.py which creates snapshots of real
    HTTP requests and responses, and during the test process tries to match
    requests with the snapshots. Sometimes it's hard to run an appropriate test
    environment locally, so we allow to use our servers to create snapshots
    for new tests, by "normalizing" the snapshots via patching hosts/credentials
    on-the-fly before saving it (see #112).

    The problem here is that we patch only requests data and not responses data,
    which is pretty difficult to unify over the whole client. It means that if
    some test gets data from API (say, a new job ID) and uses it to form another
    requests (get the job data), it will form the HTTP requests differently,
    thus it won't match with the snapshots during the test process and the tests
    will fail.

    As a temporary workaround, the helper gets a Job instance, extracts its key,
    replaces the project ID part with TEST_PROJECT_ID, and returns a new Job.
    So, the other requests done via the new job instance (updating job items,
    accessing job logs, etc) will be done using proper URLs matching with
    existing snapshots.
    """
    normalized_key = '{}/{}'.format(TEST_PROJECT_ID, job.key.split('/', 1)[1])
    return job._client.get_job(normalized_key)
