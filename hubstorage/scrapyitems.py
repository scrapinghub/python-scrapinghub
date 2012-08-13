import os
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.xlib.pydispatch import dispatcher
from hubstorage import Client

class HubStorage(object):
    """Extension to write scraped items to HubStorage"""

    def __init__(self, url):
        if 'SCRAPY_PROJECT_ID' not in os.environ or 'SCRAPY_JOB' not in os.environ:
            raise NotConfigured
        self.client = Client(url=url) # TODO: authentication and api keys
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.item_scraped, signals.item_scraped)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    @classmethod
    def from_settings(cls, settings):
        url = settings.get('HUBSTORAGE_URL', 'http://localhost:8002')
        return cls(url)

    def spider_opened(self, spider):
        spidername = os.environ.get('SCRAPY_SPIDER', spider.name)
        path = "/items/%s/%s/%s" % (os.environ['SCRAPY_PROJECT_ID'], spidername,
            os.environ['SCRAPY_JOB'])
        self.writer = self.client.open_item_writer(path)

    def item_scraped(self, item, spider):
        item = dict(item)
        item.pop('_id', None)
        item.pop('_jobid', None)
        self.writer.write_item(item)

    def spider_closed(self, spider):
        self.writer.close()
