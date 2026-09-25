from typing import Any

import mock

from scrapinghub.legacy import RequestProxyMixin


def test_requestproxymixin_add_params(proxy_mixin: RequestProxyMixin) -> None:
    params = {'param': 'val'}
    assert proxy_mixin._add_params(params) == params


def test_requestproxymixin_get(proxy_mixin: RequestProxyMixin) -> None:
    proxy_mixin._request_proxy = mock.Mock()  # type: ignore[misc]
    proxy_mixin._request_proxy._get.return_value = 'expected'
    args = ('method', 'json')
    kwargs: dict[str, Any] = {'params': {}, 'headers': {}, 'raw': True}
    assert proxy_mixin._get(*args, **kwargs) == 'expected'
    assert proxy_mixin._request_proxy._get.call_args_list == [
        (('method', 'json', {}, {}, True), {})]


def test_requestproxymixin_post(proxy_mixin: RequestProxyMixin) -> None:
    proxy_mixin._request_proxy = mock.Mock()  # type: ignore[misc]
    proxy_mixin._request_proxy._post.return_value = 'expected'
    args = ('method', 'json')
    kwargs: dict[str, Any] = {'params': {}, 'headers': {}, 'raw': True,
                              'files': {}}
    assert proxy_mixin._post(*args, **kwargs) == 'expected'
    assert proxy_mixin._request_proxy._post.call_args_list == [
        (('method', 'json', {}, {}, True, {}), {})]
