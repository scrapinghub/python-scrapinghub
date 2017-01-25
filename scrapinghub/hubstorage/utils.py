import six
import time
from six.moves.queue import Empty


def urlpathjoin(*parts):
    """Join multiple paths into a single url

    >>> urlpathjoin('https://storage.scrapinghub.com:8002/', 'jobs', '1/2/3')
    'https://storage.scrapinghub.com:8002/jobs/1/2/3'
    >>> urlpathjoin('https://storage.scrapinghub.com:8002', 'jobs', '1/2/3', None)
    'https://storage.scrapinghub.com:8002/jobs/1/2/3'
    >>> urlpathjoin(_, 'state')
    'https://storage.scrapinghub.com:8002/jobs/1/2/3/state'
    >>> urlpathjoin(_, None)
    'https://storage.scrapinghub.com:8002/jobs/1/2/3/state'
    >>> urlpathjoin(78)
    '78'
    >>> urlpathjoin('78')
    '78'
    >>> urlpathjoin('s', 78)
    's/78'
    >>> urlpathjoin('s', 78, 'foo')
    's/78/foo'
    >>> urlpathjoin('s/78/foo')
    's/78/foo'
    >>> urlpathjoin((111, 'jobs'), 33)
    '111/jobs/33'
    >>> urlpathjoin('http://localhost:8003/', ('jobs', '1111111'), '2/1')
    'http://localhost:8003/jobs/1111111/2/1'

    """
    url = None
    for p in parts:
        if p is None:
            continue
        elif isinstance(p, tuple):
            p = urlpathjoin(*p)
        elif not isinstance(p, six.text_type):
            p = six.text_type(p)

        url = p if url is None else u'{0}/{1}'.format(url.rstrip(u'/'), p)

    return url


def xauth(auth):
    """Expand authentification token

    >>> xauth(None)
    >>> xauth(('user', 'pass'))
    ('user', 'pass')
    >>> xauth('user:pass')
    ('user', 'pass')
    >>> xauth('apikey')
    ('apikey', '')

    """
    if auth is None or isinstance(auth, tuple):
        return auth
    else:
        u, _, p = auth.partition(':')
        return u, p


def millitime(*a, **kw):
    """The difference, measured in milliseconds, between the current time
    and midnight, January 1, 1970 UTC.

    >>> e = millitime()
    >>> type(e)
    <type 'int'>
    """
    ts = time.time(*a, **kw)
    return int(ts * 1000)


class iterqueue(object):
    """Iterate a queue til a maximum number of messages are read or the queue is empty

    it exposes an attribute "count" with the number of messages read

    >>> from six.moves.queue import Queue
    >>> q = Queue()
    >>> for x in range(10):
    ...     q.put(x)
    >>> qiter = iterqueue(q)
    >>> list(qiter)
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> qiter.count
    10

    >>> for x in range(10):
    ...     q.put(x)
    >>> qiter = iterqueue(q, maxcount=4)
    >>> list(qiter)
    [0, 1, 2, 3]
    >>> qiter.count
    4
    """

    def __init__(self, queue, maxcount=None):
        self.queue = queue
        self.maxcount = maxcount
        self.count = 0

    def __iter__(self):
        while (self.maxcount is None) or (self.count < self.maxcount):
            try:
                yield self.queue.get_nowait()
                self.count += 1
            except Empty:
                break


def apipoll(endpoint, *args, **kwargs):
    """Poll an api endpoint until there is a result that is not None

    poll_wait and max_poll can be specified in kwargs to set the polling
    interval and max wait time in seconds.
    """
    result = endpoint(*args, **kwargs)
    if result is not None:
        return result
    start = time.time()
    while True:
        poll_wait = kwargs.get('poll_wait', 1)
        max_poll = kwargs.get('max_poll', 60)
        time.sleep(poll_wait)
        result = endpoint(*args, **kwargs)
        if result is not None or (time.time() - start) > max_poll:
            return result


def sizeof_fmt(num):
    """Little helper to get size in human readable form.

    Size is rounded to a closest integer value (for simplicity).

    >>> sizeof_fmt(100)
    '100 B'
    >>> sizeof_fmt(1024)
    '1 KiB'
    >>> sizeof_fmt(1024*1024 + 100)
    '1 MiB'
    """
    for unit in ['B', 'KiB', 'MiB']:
        if abs(num) < 1024.0:
            return "%.0f %s" % (num, unit)
        num /= 1024.0
    return "%.0f %s" % (num, 'GiB')
