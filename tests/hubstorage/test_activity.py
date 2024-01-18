"""
Test Activity
"""


def test_post_and_reverse_get(hsproject):
    # make some sample data
    orig_data = [{'foo': 42, 'counter': i} for i in range(20)]
    data1 = orig_data[:10]
    data2 = orig_data[10:]

    # put ordered data in 2 separate posts
    hsproject.activity.post(data1)
    hsproject.activity.post(data2)

    # read them back in reverse chronological order
    result = list(hsproject.activity.list(count=20))
    assert len(result) == 20
    assert orig_data[::-1] == result


def test_filters(hsproject):
    hsproject.activity.post({'c': i} for i in range(10))
    r = list(hsproject.activity.list(filter='["c", ">", [5]]', count=2))
    assert r == [{'c': 9}, {'c': 8}]


def test_timestamp(hsproject):
    hsproject.activity.add({'foo': 'bar'}, baz='qux')
    entry = next(hsproject.activity.list(count=1, meta='_ts'))
    assert entry.pop('_ts', None)
    assert entry == {'foo': 'bar', 'baz': 'qux'}
