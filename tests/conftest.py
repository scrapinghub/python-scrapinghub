# -*- coding: utf-8 -*-


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
