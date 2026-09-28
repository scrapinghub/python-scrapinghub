from scrapinghub import HubstorageClient


def test_hubstorage_jobq_defaults_to_storage_endpoint(monkeypatch):
    monkeypatch.delenv('SHUB_JOBQ', raising=False)
    client = HubstorageClient(auth='apikey', endpoint='https://storage.example/')
    assert client.jobq.url.startswith('https://storage.example/')


def test_hubstorage_jobq_endpoint_uses_env_var(monkeypatch):
    monkeypatch.setenv('SHUB_JOBQ', 'https://jobq-internal.zyte.com/')
    client = HubstorageClient(auth='apikey', endpoint='https://storage.example/')
    assert client.jobq.url.startswith('https://jobq-internal.zyte.com/')


def test_hubstorage_jobq_endpoint_argument(monkeypatch):
    monkeypatch.delenv('SHUB_JOBQ', raising=False)
    client = HubstorageClient(
        auth='apikey',
        endpoint='https://storage.example/',
        jobq_endpoint='https://jobq.example/',
    )
    assert client.jobq.url.startswith('https://jobq.example/')


def test_hubstorage_connection_timeout_positional_compatibility(monkeypatch):
    monkeypatch.delenv('SHUB_JOBQ', raising=False)
    client = HubstorageClient('apikey', 'https://storage.example/', 12)
    assert client.connection_timeout == 12
    assert client.jobq.url.startswith('https://storage.example/')


def test_project_and_job_jobq_use_configured_endpoint():
    client = HubstorageClient(
        auth='apikey',
        endpoint='https://storage.example/',
        jobq_endpoint='https://jobq.example/',
    )
    project = client.get_project('123')
    job = client.get_job('123/1/1')

    assert project.jobq.url.startswith('https://jobq.example/')
    assert job.jobq.url.startswith('https://jobq.example/')
