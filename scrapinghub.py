"""Scrapinghub API Client Library"""

import os
import json
import warnings
import requests
from cStringIO import StringIO

from requests.compat import urljoin
from requests.models import urlencode


__all__ = ["APIError", "Connection"]


class Connection(object):
    """Main class to access Scrapinghub API.
    """

    API_METHODS = {
        'addversion': 'scrapyd/addversion',
        'listprojects': 'scrapyd/listprojects',
        'jobs_count': 'jobs/count',
        'jobs_list': 'jobs/list',
        'jobs_update': 'jobs/update',
        'jobs_delete': 'jobs/delete',
        'eggs_add': 'eggs/add',
        'eggs_delete': 'eggs/delete',
        'eggs_list': 'eggs/list',
        'as_extract': 'as/extract',
        'schedule': 'schedule',
        'items': 'items',
        'log': 'log',
        'spiders': 'spiders/list',
        'reports_add': 'reports/add',
    }

    def __init__(self, username_or_apikey=None, password='', _old_passwd='', url='http://panel.scrapinghub.com/api/'):
        if username_or_apikey is None:
            username_or_apikey = os.environ.get('SH_APIKEY')
            if username_or_apikey is None:
                raise RuntimeError("No API key provided and SH_APIKEY environment variable not set")
        if username_or_apikey.startswith('http://'):
            warnings.warn("Instantiating scrapinghub.Connection with url as first argument is deprecated", stacklevel=2)
            url, username_or_apikey, password = username_or_apikey, password, _old_passwd
        self.url = url
        self.username_or_apikey = username_or_apikey
        self.auth = (username_or_apikey, password)
        self._request_headers = {'User-Agent': 'python-scrapinghub/1.0'}

    def __repr__(self):
        return "Connection(%r)" % self.username_or_apikey

    def _build_url(self, method, format):
        """Returns full url for given method and format"""
        # TODO: verify method's format support
        try:
            base_path = self.API_METHODS[method]
        except KeyError:
            raise APIError("Unknown method: {0}".format(method))
        else:
            path = "{0}.{1}".format(base_path, format)
            return urljoin(self.url, path)

    def _get(self, method, format, params=None, headers=None, raw=False):
        """Performs GET request"""
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
        if format not in ('json', 'jl'):
            raise APIError("format must be either json or jl")

        request_headers = self._request_headers.copy()
        if headers:
            request_headers.update(headers)

        if data is None and files is None:
            response = requests.get(url, headers=request_headers, auth=self.auth)
        else:
            response = requests.post(url, headers=request_headers, auth=self.auth, data=data, files=files)
        return self._decode_response(response, format, raw)

    def _decode_response(self, response, format, raw):
        if raw:
            return response.raw
        elif format == 'json':
            data = json.loads(response.text)
            # validate response
            try:
                if data['status'] == 'ok':
                    return data
                elif data['status'] in ('error', 'badrequest'):
                    raise APIError(data['message'])
                else:
                    raise APIError("Unknown response status: {0[status]}".format(data))
            except KeyError:
                raise APIError("JSON response does not contain status")
        else: # jl
            return (json.loads(line) for line in response.iter_lines())

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


class RequestProxyMixin:

    def _add_params(self, params):
        return params

    def _get(self, method, format, params=None, headers=None, raw=False):
        params = self._add_params(params or {})
        return self._request_proxy._get(method, format, params, headers, raw)

    def _post(self, method, format, params=None, headers=None, raw=False, files=None):
        params = self._add_params(params or {})
        return self._request_proxy._post(method, format, params, headers, raw, files)


class Project(object, RequestProxyMixin):
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name

    def __repr__(self):
        return "Project({0.connection!r}, {0.name})".format(self)

    def schedule(self, spider, **params):
        params['spider'] = spider
        result = self._post('schedule', 'json', params)
        return result['jobid']

    def jobs(self, **params):
        return JobSet(self, **params)

    def job(self, id):
        try:
            return iter(self.jobs(job=id, count=1)).next()
        except StopIteration:
            return None

    def spiders(self, **params):
        result = self._get('spiders', 'json', params)
        return result['spiders']

    @property
    def _request_proxy(self):
        return self.connection

    def _add_params(self, params):
        # force project param
        params.update(project=self.name)
        return params


class JobSet(object, RequestProxyMixin):

    def __init__(self, project, **params):
        self.project = project
        self.params = params
        # jobs one-shot iterator
        self._jobs = None

    def __repr__(self):
        params = ', '.join("{0}={1}".format(*i) for i in self.params.iteritems())
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

    def delete(self):
        for job in self:
            job.delete()

    def _load_jobs(self):
        # only load once
        if self._jobs is None:
            result = self._get('jobs_list', 'jl', self.params)
            try:
                status_line = result.next()
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


class Job(object, RequestProxyMixin):
    def __init__(self, project, id, info):
        self.project = project
        self._id = id
        self.info = info

    @property
    def id(self):
        return self._id

    def __repr__(self):
        return "Job({0.project!r}, {0.id})".format(self)

    def items(self):
        return self._get('items', 'jl')

    def update(self, **modifiers):
        # XXX: only allow add_tag/remove_tag
        result = self._post('jobs_update', 'json', modifiers)
        return result['count']

    def delete(self):
        result = self._post('jobs_delete', 'json')
        return result['count']

    def add_report(self, key, content, content_type='text/plain'):
        params = {
            'project': self.project.name,
            'job': self.id,
            'key': key,
            'content_type': content_type,
        }
        files = {'content': ('report', StringIO(content))}
        self._post('reports_add', 'json', params, files=files)

    @property
    def _request_proxy(self):
        return self.project

    def _add_params(self, params):
        params['job'] = self.id
        return params


class APIError(Exception):
    pass

