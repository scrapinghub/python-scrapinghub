from .resourcetype import ResourceType

# TODO: remove backwards compatible methods


class Activity(ResourceType):

    resource_type = 'activity'

    def list(self, **params):
        return self.apiget(params=params)
    get = list

    def post(self, _value, **params):
        return self.apipost(jl=_value, params=params)

    def add(self, *args, **kwargs):
        entry = dict(*args, **kwargs)
        return self.post(entry)
