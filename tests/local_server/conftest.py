import pytest

from scrapinghub import ScrapinghubClient
from scrapinghub.client import HubstorageClient

TIMEOUT = 1


@pytest.fixture
def make_local_client(httpserver, monkeypatch):
    """Return a factory of clients that send all requests to *httpserver*,
    retry idempotent requests up to 3 times without waiting in between, and
    time out after :data:`TIMEOUT` seconds.

    Keyword arguments override those passed to
    :class:`~scrapinghub.ScrapinghubClient`."""
    monkeypatch.setattr(HubstorageClient, "RETRY_DEFAULT_JITTER_MS", 0)
    monkeypatch.setattr(
        HubstorageClient, "RETRY_DEFAULT_EXPONENTIAL_BACKOFF_MS", 0
    )

    def make(**kwargs):
        kwargs = {
            "auth": "a" * 32,
            "endpoint": httpserver.url_for("/"),
            "jobq_endpoint": httpserver.url_for("/"),
            "dash_endpoint": httpserver.url_for("/api/"),
            "connection_timeout": TIMEOUT,
            "max_retries": 3,
            **kwargs,
        }
        return ScrapinghubClient(**kwargs)

    return make


@pytest.fixture
def local_client(make_local_client):
    return make_local_client()
