from .resourcetype import ResourceType
from .utils import millitime


class Activity(ResourceType):

    resource_type = 'activity'

    def get(self, **params):
        return self.apiget(params=params)

    def post(self, _value, **params):
        return self.apipost(jl=_value, params=params)

    def add(self, **params):
        params['timestamp'] = params.get('timestamp') or millitime()
        return self.post(params)
