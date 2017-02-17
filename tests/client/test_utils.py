import pytest
from scrapinghub.utils import format_iter_filters


def test_format_iter_filters():
    # work with empty params
    assert format_iter_filters({}) == {}

    # doesn't affect other params
    params = {'a': 123, 'b': 456}
    assert format_iter_filters(params) == params

    # pass filter as-is if not list
    params = {'filter': 'some-string'}
    assert format_iter_filters(params) == params

    # work fine with empty filter
    params = {'filter': []}
    assert format_iter_filters(params) == params

    # pass string filters as-is
    params = {'filter': ['str1', 'str2']}
    assert format_iter_filters(params) == params

    # converts list-formatted filters
    params = {'filter': [['field', '>=', ['val']], 'filter2']}
    assert (format_iter_filters(params) ==
            {'filter': ['["field", ">=", ["val"]]', 'filter2']})

    # works the same with tuple entries
    params = {'filter': [('field', '==', ['val'])]}
    assert (format_iter_filters(params) ==
            {'filter': ['["field", "==", ["val"]]']})

    # exception if entry is not list/tuple or string
    with pytest.raises(ValueError):
        format_iter_filters({'filter': ['test', 123]})
