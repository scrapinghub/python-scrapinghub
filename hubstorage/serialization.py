import six
from json import dumps, loads
from datetime import datetime

EPOCH = datetime.utcfromtimestamp(0)
ADAYINSECONDS = 24 * 3600


def jlencode(iterable):
    if isinstance(iterable, (dict, six.string_types)):
        iterable = [iterable]
    return u'\n'.join(jsonencode(o) for o in iterable)


def jldecode(lineiterable):
    for line in lineiterable:
        yield loads(line)


def jsonencode(o):
    return dumps(o, default=jsondefault)


def jsondefault(o):
    if isinstance(o, datetime):
        delta = o - EPOCH
        u = delta.microseconds
        s = delta.seconds
        d = delta.days
        return (u + (s + d * ADAYINSECONDS) * 1e6) // 1000
    else:
        return six.text_type(o)
