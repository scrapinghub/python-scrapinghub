import six
from json import dumps, loads
from datetime import datetime

EPOCH = datetime.utcfromtimestamp(0)
ADAYINSECONDS = 24 * 3600


try:
    from msgpack import Unpacker
    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False


def jlencode(iterable):
    if isinstance(iterable, (dict, six.string_types)):
        iterable = [iterable]
    return u'\n'.join(jsonencode(o) for o in iterable)


def jldecode(lineiterable):
    for line in lineiterable:
        yield loads(line)


def mpdecode(iterable):
    unpacker = Unpacker(encoding='utf8')
    for chunk in iterable:
        unpacker.feed(chunk)
        # Each chunk can have none or many objects,
        # so here we dispatch any object ready
        for obj in unpacker:
            yield obj


def jsonencode(o):
    return dumps(o, default=jsondefault)


def jsondefault(o):
    if isinstance(o, datetime):
        # convert TZ-aware datetime object to POSIX timestamp
        if o.tzinfo:
            o = o.replace(tzinfo=None) - o.utcoffset()
        delta = o - EPOCH
        u = delta.microseconds
        s = delta.seconds
        d = delta.days
        return (u + (s + d * ADAYINSECONDS) * 1e6) // 1000
    else:
        return six.text_type(o)
