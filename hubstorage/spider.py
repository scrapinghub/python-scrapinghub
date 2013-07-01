from .resourcetype import ResourceType
from .utils import urlpathjoin


class Spider(ResourceType):

    resource_type = 'spiders'

    def lastjobsummary(self, **params):
        return self.apiget('lastjobsummary', params=params)
