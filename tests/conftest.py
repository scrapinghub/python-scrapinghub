# -*- coding: utf-8 -*-
import pytest
import re

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


def request_accept_header_matcher(r1, r2):
    """Custom VCR.py matcher by Accept header."""

    def _get_accept_header(request):
        return request.headers.get('Accept', '').lower()

    return _get_accept_header(r1) == _get_accept_header(r2)


@pytest.fixture
def hsfrontier_name(request):
    """Provide a name for test-unique HS frontier."""
    return re.sub('\W+', '-', request.node.nodeid)
