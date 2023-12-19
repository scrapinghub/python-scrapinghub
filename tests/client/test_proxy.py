import mock
import pytest

from scrapinghub.client.proxy import _format_iter_filters
from scrapinghub.client.proxy import _ItemsResourceProxy


def test_format_iter_filters():
    # work with empty params
    assert _format_iter_filters({}) == {}

    # doesn't affect other params
    params = {'a': 123, 'b': 456}
    assert _format_iter_filters(params) == params

    # pass filter as-is if not list
    params = {'filter': 'some-string'}
    assert _format_iter_filters(params) == params

    # work fine with empty filter
    params = {'filter': []}
    assert _format_iter_filters(params) == params

    # pass string filters as-is
    params = {'filter': ['str1', 'str2']}
    assert _format_iter_filters(params) == params

    # converts list-formatted filters
    params = {'filter': [['field', '>=', ['val']], 'filter2']}
    assert (_format_iter_filters(params) ==
            {'filter': ['["field", ">=", ["val"]]', 'filter2']})

    # works the same with tuple entries
    params = {'filter': [('field', '==', ['val'])]}
    assert (_format_iter_filters(params) ==
            {'filter': ['["field", "==", ["val"]]']})

    # exception if entry is not list/tuple or string
    with pytest.raises(ValueError):
        _format_iter_filters({'filter': ['test', 123]})


def test_item_resource_iter_no_params():
    class MockClient:
        def __init__(self):
            self._hsclient = object()

    items_proxy = _ItemsResourceProxy(mock.Mock, MockClient(), 'mocked_key')
    items_proxy._origin = mock.Mock()
    items_proxy.iter(count=123)
    assert items_proxy._origin.list.call_args == mock.call(None, count=123)


def test_item_resource_iter_with_params():
    class MockClient:
        def __init__(self):
            self._hsclient = object()

    items_proxy = _ItemsResourceProxy(mock.Mock, MockClient(), 'mocked_key')
    items_proxy._origin = mock.Mock()
    items_proxy.iter(count=123, startts=12345)
    assert (items_proxy._origin.list.call_args ==
            mock.call(None, count=123, startts=12345))
