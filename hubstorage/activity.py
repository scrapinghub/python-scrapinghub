from .resourcetype import ResourceType


class Activity(ResourceType):

    resource_type = 'activity'

    def get(self, **params):
        return self.apiget(params=params)

    def post(self, _value, **params):
        return self.apipost(jl=_value, params=params)
