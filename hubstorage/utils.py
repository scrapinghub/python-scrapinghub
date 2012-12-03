import time
from urlparse import urljoin


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

    """
    url = parts[0]
    for part in parts[1:]:
        if part is not None:
            url = urljoin(url.rstrip('/') + '/', str(part))
    return url

def xauth(auth):
    """Expand authentification token

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

