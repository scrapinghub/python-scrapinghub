import time
from Queue import Empty


def urlpathjoin(*parts):
    """Join multiple paths into a single url

    >>> urlpathjoin('http://storage.scrapinghub.com:8002/', 'jobs', '1/2/3')
    'http://storage.scrapinghub.com:8002/jobs/1/2/3'
    >>> urlpathjoin('http://storage.scrapinghub.com:8002', 'jobs', '1/2/3', None)
    'http://storage.scrapinghub.com:8002/jobs/1/2/3'
    >>> urlpathjoin(_, 'state')
    'http://storage.scrapinghub.com:8002/jobs/1/2/3/state'
    >>> urlpathjoin(_, None)
    'http://storage.scrapinghub.com:8002/jobs/1/2/3/state'
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
        elif isinstance(p, unicode):
            p = p.encode('utf8')
        elif not isinstance(p, str):
            p = str(p)

        url = p if url is None else '{0}/{1}'.format(url.rstrip('/'), p)

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
    return int(time.time(*a, **kw) * 1000)


class iterqueue(object):
    """Iterate a queue til a maximum number of messages are read or the queue is empty

    it exposes an attribute "count" with the number of messages read
    """

    def __init__(self, queue, maxcount):
        self.queue = queue
        self.maxcount = maxcount
        self.count = 0

    def __iter__(self):
        while not self.maxcount or self.count < self.maxcount:
            try:
                yield self.queue.get_nowait()
                self.count += 1
                self.queue.task_done()
            except Empty:
                break
