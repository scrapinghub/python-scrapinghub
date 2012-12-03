from .resourcetype import ResourceType


class Activity(ResourceType):

    resource_type = 'activity'

    def get(self, **params):
        return self.apiget(params=params)
