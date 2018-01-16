"""
Scrapinghub API Client Library
"""

from __future__ import division, print_function, absolute_import
import os
import sys
import json
import logging
import socket
import time
import warnings


# Python 2/3 compatibility
_IS_PYTHON2 = sys.version_info < (3,)
if _IS_PYTHON2:
    import httplib
    _BINARY_TYPE = str
    range = xrange
else:
    import http.client as httplib
    _BINARY_TYPE = bytes


logger = logging.getLogger('scrapinghub')


class Connection(object):
    """Main class to access Scrapinghub API.
    """

    DEFAULT_ENDPOINT = 'https://app.scrapinghub.com/api/'

    API_METHODS = {
        'addversion': 'scrapyd/addversion',
        'listprojects': 'scrapyd/listprojects',
        'jobs_count': 'jobs/count',
        'jobs_list': 'jobs/list',
        'jobs_update': 'jobs/update',
        'jobs_stop': 'jobs/stop',
        'jobs_delete': 'jobs/delete',
        'eggs_add': 'eggs/add',
        'eggs_delete': 'eggs/delete',
        'eggs_list': 'eggs/list',
        'as_project_slybot': 'as/project-slybot',
        'as_spider_properties': 'as/spider-properties',
        'run': 'run',
        'schedule': 'schedule',  # deprecated in favour of run
        'items': 'items',
        'log': 'log',
        'spiders': 'spiders/list',
        'reports_add': 'reports/add',
    }

    def __init__(self, apikey=None, password='', _old_passwd='',
                 url=None, connection_timeout=None):
        if apikey is None:
            apikey = os.environ.get('SH_APIKEY')
            if apikey is None:
                raise RuntimeError("No API key provided and SH_APIKEY environment variable not set")

        assert not apikey.startswith('http://'), \
                "Instantiating scrapinghub.Connection with url as first argument is not supported"
        if password:
            warnings.warn("A lot of endpoints support authentication only via apikey.")
        self.apikey = apikey
        self.password = password or ''
        self.url = url or self.DEFAULT_ENDPOINT
        self._session = self._create_session()
        self._connection_timeout = connection_timeout

    def __repr__(self):
        return "Connection(%r)" % self.apikey

    @property
    def auth(self):
        warnings.warn("'auth' connection attribute is deprecated, "
                      "use 'apikey' attribute instead", stacklevel=2)
        return (self.apikey, self.password)

    def _create_session(self):
        from requests import session
        from scrapinghub import __version__
        s = session()
        s.auth = (self.apikey, self.password)
        s.headers.update({
            'User-Agent': 'python-scrapinghub/{0}'.format(__version__),
        })
        # For python-requests >= 1.x
        s.stream = True
        # For python-requests < 1.x
        s.prefetch = False
        return s

    def _build_url(self, method, format):
        """Returns full url for given method and format"""
        from requests.compat import urljoin
        # TODO: verify method's format support
        try:
            base_path = self.API_METHODS[method]
        except KeyError:
            raise APIError("Unknown method: {0}".format(method),
                           _type=APIError.ERR_VALUE_ERROR)
        else:
            path = "{0}.{1}".format(base_path, format)
            return urljoin(self.url, path)

    def _get(self, method, format, params=None, headers=None, raw=False):
        """Performs GET request"""
        from requests.compat import urlencode
        url = self._build_url(method, format)
        if params:
            url = "{0}?{1}".format(url, urlencode(params, True))
        return self._request(url, None, headers, format, raw)

    def _post(self, method, format, params=None, headers=None, raw=False, files=None):
        """Performs POST request"""
        url = self._build_url(method, format)
        return self._request(url, params, headers, format, raw, files)

    def _request(self, url, data, headers, format, raw, files=None):
        """Performs the request using and returns the content deserialized,
        based on given `format`.

        Available formats:
            * json - Returns a json object and checks for errors
            * jl   - Returns a generator of json object per item

        Raises APIError if json response have error status.
        """
        if format not in ('json', 'jl') and not raw:
            raise APIError("format must be either json or jl",
                           _type=APIError.ERR_VALUE_ERROR)

        if data is None and files is None:
            response = self._session.get(url, headers=headers,
                                         timeout=self._connection_timeout)
        else:
            response = self._session.post(url, headers=headers,
                                          data=data, files=files,
                                          timeout=self._connection_timeout)
        return self._decode_response(response, format, raw)

    def _decode_response(self, response, format, raw):
        if response.status_code == 404:
            raise APIError("Not found", _type=APIError.ERR_NOT_FOUND)
        elif 500 <= response.status_code < 600:
            raise APIError("Internal server error",
                           _type=APIError.ERR_SERVER_ERROR)
        if raw:
            return response.raw
        elif format == 'json':
            data = json.loads(response.text)
            # validate response
            try:
                if data['status'] == 'ok':
                    return data
                elif (data['status'] == 'error' and
                        data['message'] == 'Authentication failed'):
                    raise APIError(data['message'],
                                   _type=APIError.ERR_AUTH_ERROR)
                elif data['status'] in ('error', 'badrequest'):
                    raise APIError(data['message'],
                                   _type=APIError.ERR_BAD_REQUEST)
                else:
                    raise APIError("Unknown response status: {0[status]}".format(data))
            except KeyError:
                raise APIError("JSON response does not contain status")
        else:  # jl
            return (json.loads(line.decode('utf-8')
                              if isinstance(line, _BINARY_TYPE) else line)
                        for line in response.iter_lines())

    ##
    ## public methods
    ##
    def __getitem__(self, key):
        """Returns `Project` instance for given key.

        Does not verify if project exists.
        """
        return Project(self, key)

    def project_ids(self):
        """Returns a list of projects available for this connection and
        crendentials.
        """
        result = self._get('listprojects', 'json')
        return result['projects']

    def project_names(self):
        warnings.warn("scrapinghub.Connection.project_names() method is deprecated, use project_ids() method instead", stacklevel=2)
        return self.project_ids()


class RequestProxyMixin(object):

    def _add_params(self, params):
        return params

    def _get(self, method, format, params=None, headers=None, raw=False):
        params = self._add_params(params or {})
        return self._request_proxy._get(method, format, params, headers, raw)

    def _post(self, method, format, params=None, headers=None, raw=False, files=None):
        params = self._add_params(params or {})
        return self._request_proxy._post(method, format, params, headers, raw, files)


class Project(RequestProxyMixin):
    def __init__(self, connection, projectid):
        self.connection = connection
        self.id = projectid

    def __repr__(self):
        return "Project({0.connection!r}, {0.id})".format(self)

    @property
    def name(self):
        warnings.warn("Project.name is deprecated, use Project.id instead", stacklevel=2)
        return self.id

    def schedule(self, spider, **params):
        params['spider'] = spider
        result = self._post('schedule', 'json', params)
        return result['jobid']

    def jobs(self, **params):
        return JobSet(self, **params)

    def job(self, id):
        for x in self.jobs(job=id, count=1):
            return x

    def spiders(self, **params):
        result = self._get('spiders', 'json', params)
        return result['spiders']

    @property
    def _request_proxy(self):
        return self.connection

    def _add_params(self, params):
        # force project param
        params.update(project=self.id)
        return params

    def autoscraping_project_slybot(self, spiders=(), outputfile=None):
        from shutil import copyfileobj
        params = {}
        if spiders:
            params['spider'] = spiders
        r = self._get('as_project_slybot', 'zip', params, raw=True)
        return r if outputfile is None else copyfileobj(r, outputfile)

    def autoscraping_spider_properties(self, spider, start_urls=None):
        params = {'spider': spider}
        if start_urls:
            params['start_url'] = start_urls
            return self._post('as_spider_properties', 'json', params)
        return self._get('as_spider_properties', 'json', params)


class JobSet(RequestProxyMixin):

    def __init__(self, project, **params):
        self.project = project
        self.params = params
        # jobs one-shot iterator
        self._jobs = None

    def __repr__(self):
        params = ', '.join("{0}={1}".format(*i) for i in self.params.items())
        return "JobSet({0.project!r}, {1})".format(self, params)

    def __iter__(self):
        self._load_jobs()
        return (Job(self.project, info['id'], info) for info in self._jobs)

    def count(self):
        """Returns total results count of current filters.
        Does not inclue `count` neither `offset`.
        """
        result = self._get('jobs_count', 'json')
        return result['total']

    def update(self, **modifiers):
        params = self.params.copy()
        params.update(modifiers)
        result = self._post('jobs_update', 'json', params)
        return result['count']

    def stop(self):
        for job in self:
            job.stop()

    def delete(self):
        for job in self:
            job.delete()

    def _load_jobs(self):
        # only load once
        if self._jobs is None:
            result = self._get('jobs_list', 'jl', self.params)
            try:
                status_line = next(result)
            except StopIteration:
                raise APIError("JSON response does not contain status")
            else:
                # jl status expected is only "ok"
                status = status_line.get('status', '')
                if status != 'ok':
                    raise APIError("Unknown response status: {0}".format(status))

                self._jobs = result

    @property
    def _request_proxy(self):
        return self.project

    def _add_params(self, params):
        # default to JobSet's params
        params2 = self.params.copy()
        # update with user params
        params2.update(params)
        return params2


class Job(RequestProxyMixin):

    MAX_RETRIES = 180
    RETRY_INTERVAL = 60

    def __init__(self, project, id, info):
        self.project = project
        self._id = id
        self.info = info

    @property
    def id(self):
        return self._id

    def __repr__(self):
        return "Job({0.project!r}, {0.id})".format(self)

    def items(self, offset=0, count=None, meta=None):
        import requests
        params = {'offset': offset}
        if meta is not None:
            params['meta'] = meta
        if count is not None:
            params['count'] = count

        lastexc = None
        for attempt in range(self.MAX_RETRIES):
            retrieved = 0
            try:
                for item in self._get('items', 'jl', params=params):
                    yield item
                    retrieved += 1
                break
            except (ValueError, socket.error, requests.RequestException, httplib.HTTPException) as exc:
                lastexc = exc
                params['offset'] += retrieved
                if 'count' in params:
                    params['count'] -= retrieved
                logger.debug("Retrying read of items.jl in %ds: job=%s offset=%d count=%d"
                             "attempt=%d/%d error=%s",
                             self.RETRY_INTERVAL, self._id, params['offset'],
                             params.get('count'), attempt, self.MAX_RETRIES, exc)
                time.sleep(self.RETRY_INTERVAL)
        else:
            logger.error('Failed %d times reading items from %s, last error was: %s',
                         self.MAX_RETRIES, self._id, lastexc)

    def update(self, **modifiers):
        # XXX: only allow add_tag/remove_tag
        result = self._post('jobs_update', 'json', modifiers)
        return result['count']

    def stop(self):
        result = self._post('jobs_stop', 'json')
        return result['status'] == 'ok'

    def delete(self):
        result = self._post('jobs_delete', 'json')
        return result['count']

    def add_report(self, key, content, content_type='text/plain'):
        from requests.compat import StringIO
        params = {
            'project': self.project.id,
            'job': self.id,
            'key': key,
            'content_type': content_type,
        }
        files = {'content': ('report', StringIO(content))}
        self._post('reports_add', 'json', params, files=files)

    def log(self, **params):
        return self._get('log', 'jl', params)

    @property
    def _request_proxy(self):
        return self.project

    def _add_params(self, params):
        params['job'] = self.id
        return params


class APIError(Exception):

    ERR_DEFAULT = "err_default"
    ERR_NOT_FOUND = "err_not_found"
    ERR_VALUE_ERROR = "err_value_error"
    ERR_BAD_REQUEST = "err_bad_request"
    ERR_AUTH_ERROR = "err_auth_error"
    ERR_SERVER_ERROR = "err_server_error"

    def __init__(self, message, _type=None):
        super(APIError, self).__init__(message)
        self._type = _type or self.ERR_DEFAULT
