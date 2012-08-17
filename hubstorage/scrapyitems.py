import os, urlparse
from scrapy import signals, log
from scrapy.exceptions import NotConfigured
from scrapy.xlib.pydispatch import dispatcher
from hubstorage import Client

class HubStorage(object):
    """Extension to write scraped items to HubStorage"""

    def __init__(self, url):
        if 'SHUB_JOB' not in os.environ:
            raise NotConfigured
        apikey = os.environ['SHUB_JOBAUTH']
        client = Client(apikey, url=url)
        path = "/items/%(SHUB_PROJECT)s/%(SHUB_SPIDER)s/%(SHUB_JOB)s" % os.environ
        self.writer = client.open_item_writer(path)
        log.msg("HubStorage: writing items to %s" % urlparse.urljoin(url, path))
        dispatcher.connect(self.item_scraped, signals.item_scraped)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    @classmethod
    def from_settings(cls, settings):
        url = settings.get('SHUB_STORAGE', 'http://localhost:8002')
        return cls(url)

    def item_scraped(self, item, spider):
        item = dict(item)
        item.pop('_id', None)
        item.pop('_jobid', None)
        self.writer.write_item(item)

    def spider_closed(self, spider):
        self.writer.close()
