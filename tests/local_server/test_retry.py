import socket
import time

import pytest
from werkzeug import Response

from scrapinghub.client.exceptions import ScrapinghubAPIError, ServerError

from .conftest import TIMEOUT

JOB_KEY = "1/2/3"
STATE_PATH = f"/jobs/{JOB_KEY}/state"


def drop_connection(request):
    request.environ["werkzeug.socket"].shutdown(socket.SHUT_RDWR)
    return Response()


def stall(request):
    time.sleep(TIMEOUT * 1.5)
    return Response('"running"')


def truncate_body(request):
    return Response(iter(['"runn']), headers={"Content-Length": "9"})


def fail(httpserver, path, times, handler):
    for _ in range(times):
        httpserver.expect_oneshot_request(path).respond_with_handler(handler)


def status(code):
    return lambda request: Response(status=code)


@pytest.mark.parametrize(
    "handler",
    [
        pytest.param(status(code), id=str(code))
        for code in (408, 429, 502, 503, 504)
    ]
    + [
        pytest.param(drop_connection, id="drop"),
        pytest.param(stall, id="stall"),
        pytest.param(
            truncate_body,
            id="truncate",
            marks=pytest.mark.xfail(
                reason="Truncated bodies are not retried", strict=True
            ),
        ),
    ],
)
def test_retried(httpserver, local_client, handler):
    fail(httpserver, STATE_PATH, 2, handler)
    httpserver.expect_request(STATE_PATH).respond_with_json("running")
    job = local_client.get_job(JOB_KEY)
    assert job.metadata.get("state") == "running"
    assert len(httpserver.log) == 3


@pytest.mark.parametrize("code", [400, 401, 403, 404, 500])
def test_not_retried_status(httpserver, local_client, code):
    fail(httpserver, STATE_PATH, 1, status(code))
    httpserver.expect_request(STATE_PATH).respond_with_json("running")
    job = local_client.get_job(JOB_KEY)
    with pytest.raises(ScrapinghubAPIError):
        job.metadata.get("state")
    assert len(httpserver.log) == 1


def test_retry_limit(httpserver, local_client):
    httpserver.expect_request(STATE_PATH).respond_with_data(status=503)
    job = local_client.get_job(JOB_KEY)
    with pytest.raises(ServerError):
        job.metadata.get("state")
    assert len(httpserver.log) == 4


def test_retries_disabled(httpserver, make_local_client):
    client = make_local_client(max_retries=0)
    httpserver.expect_request(STATE_PATH).respond_with_data(status=503)
    job = client.get_job(JOB_KEY)
    with pytest.raises(ServerError):
        job.metadata.get("state")
    assert len(httpserver.log) == 1


@pytest.mark.parametrize(
    ("path", "call"),
    [
        (f"/jobs/{JOB_KEY}/foo", lambda client: client.get_job(JOB_KEY).metadata.set("foo", "bar")),
        (f"/jobs/{JOB_KEY}/foo", lambda client: client.get_job(JOB_KEY).metadata.delete("foo")),
        (
            "/collections/1/s/foo",
            lambda client: client.get_project(1).collections.get_store("foo").set(
                {"_key": "bar"}
            ),
        ),
        (
            "/collections/1/s/foo/deleted",
            lambda client: client.get_project(1).collections.get_store("foo").delete("bar"),
        ),
    ],
    ids=["metadata-set", "metadata-delete", "store-set", "store-delete"],
)
def test_idempotent_writes_retried(httpserver, local_client, path, call):
    fail(httpserver, path, 2, status(503))
    httpserver.expect_request(path).respond_with_data()
    call(local_client)
    assert len(httpserver.log) == 3


def test_non_idempotent_write_not_retried(httpserver, local_client):
    path = f"/jobq/{JOB_KEY}/cancel"
    fail(httpserver, path, 1, status(503))
    httpserver.expect_request(path).respond_with_data()
    job = local_client.get_job(JOB_KEY)
    with pytest.raises(ServerError):
        job.cancel()
    assert len(httpserver.log) == 1
