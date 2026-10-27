import pytest
from requests import HTTPError, Response

from scrapinghub import HubstorageClient
from scrapinghub.hubstorage.resourcetype import DownloadableResource


@pytest.fixture
def collections():
    client = HubstorageClient(auth='apikey',
                              endpoint='https://storage.example/')
    return client.get_project('123').collections


def http_error(status_code, text=''):
    response = Response()
    response.status_code = status_code
    response.encoding = 'utf-8'
    response._content = text.encode('utf-8')
    return HTTPError(response=response)


def raiser(error):
    def raise_error(*args, **kwargs):
        raise error
    return raise_error


def test_get_bad_request(collections, monkeypatch):
    monkeypatch.setattr(collections, 'apiget', raiser(http_error(400, 'bad')))
    with pytest.raises(ValueError) as excinfo:
        collections.get('s', 'foo', 'key')
    assert str(excinfo.value) == 'bad'


def test_get_server_error(collections, monkeypatch):
    error = http_error(500)
    monkeypatch.setattr(collections, 'apiget', raiser(error))
    with pytest.raises(HTTPError) as excinfo:
        collections.get('s', 'foo', 'key')
    assert excinfo.value is error


def test_set_server_error(collections, monkeypatch):
    error = http_error(500)
    monkeypatch.setattr(collections, 'apipost', raiser(error))
    with pytest.raises(HTTPError) as excinfo:
        collections.set('s', 'foo', [{'_key': 'key'}])
    assert excinfo.value is error


def test_iter_msgpack(collections, monkeypatch):
    calls = []
    monkeypatch.setattr(DownloadableResource, 'iter_msgpack',
                        lambda self, path, **kwargs: calls.append((path, kwargs)))
    collections.iter_msgpack('s', 'foo', prefix='bar')
    assert calls == [(('s', 'foo'), {'requests_params': None, 'prefix': 'bar'})]


@pytest.mark.parametrize('method,coltype', [
    ('new_store', 's'),
    ('new_cached_store', 'cs'),
    ('new_versioned_store', 'vs'),
    ('new_versioned_cached_store', 'vcs'),
])
def test_new_collections(collections, method, coltype):
    collection = getattr(collections, method)('foo')
    assert (collection.coltype, collection.colname) == (coltype, 'foo')


def test_count_follows_pagination(collections, monkeypatch):
    pages = iter([{'count': 2, 'nextstart': 'key2'}, {'count': 3}])
    starts = []

    def apirequest(path, **kwargs):
        starts.append(kwargs['params'].get('start'))
        return iter([next(pages)])

    monkeypatch.setattr(collections, 'apirequest', apirequest)
    progress = []
    total = collections.count('s', 'foo', progress=lambda *args: progress.append(args))

    assert total == 5
    assert starts == [None, 'key2']
    assert progress == [(2, 'key2')]


def test_count_bad_request(collections, monkeypatch):
    monkeypatch.setattr(collections, 'apirequest', raiser(http_error(400, 'bad')))
    with pytest.raises(ValueError) as excinfo:
        collections.count('s', 'foo')
    assert str(excinfo.value) == 'bad'


def test_count_server_error(collections, monkeypatch):
    error = http_error(500)
    monkeypatch.setattr(collections, 'apirequest', raiser(error))
    with pytest.raises(HTTPError) as excinfo:
        collections.count('s', 'foo')
    assert excinfo.value is error


def test_collection_iter_json(collections, monkeypatch):
    calls = []
    monkeypatch.setattr(collections, 'iter_json',
                        lambda *args, **kwargs: calls.append((args, kwargs)))
    collections.new_store('foo').iter_json(prefix='bar')
    assert calls == [(('s', 'foo'), {'requests_params': None, 'prefix': 'bar'})]
