from .resourcetype import ResourceType
from .utils import urlpathjoin


class JobQ(ResourceType):

    resource_type = 'jobq'

    def push(self, spider, **jobparams):
        jobparams['spider'] = spider
        r = self.apipost('push', jl=jobparams)
        return r.next()

    def summary(self, _queuename=None):
        path = urlpathjoin('summary', _queuename)
        return list(self.apiget(path))
