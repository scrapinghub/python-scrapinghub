# -*- coding: utf-8 -*-
import pytest

from scrapinghub.hubstorage.serialization import MSGPACK_AVAILABLE


def pytest_addoption(parser):
    parser.addoption(
        "--update-cassettes", action="store_true", default=False,
        help="test with real services rewriting existing vcr cassettes")
    parser.addoption(
        "--ignore-cassettes", action="store_true", default=False,
        help="test with real services skipping existing vcr cassettes")
    parser.addoption(
        "--disable-msgpack", action="store_true", default=False,
        help="disable messagepack-serialization based tests")


@pytest.fixture(params=['json', 'msgpack'])
def json_and_msgpack(pytestconfig, monkeypatch, request):
    if request.param == 'json':
        monkeypatch.setattr('scrapinghub.hubstorage.resourcetype.MSGPACK_AVAILABLE', False)
        monkeypatch.setattr('scrapinghub.hubstorage.collectionsrt.MSGPACK_AVAILABLE', False)
    elif not MSGPACK_AVAILABLE or request.config.getoption("--disable-msgpack"):
        pytest.skip("messagepack-based tests are disabled")
    return request.param


def request_accept_header_matcher(r1, r2):
    """Custom VCR.py matcher by Accept header."""

    def _get_accept_header(request):
        return request.headers.get('Accept', '').lower()

    return _get_accept_header(r1) == _get_accept_header(r2)
