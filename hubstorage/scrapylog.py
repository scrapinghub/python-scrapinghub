import sys, os, time

stderr = sys.stderr

from twisted.python import log as txlog
from scrapy.conf import settings
from scrapy.utils.python import unicode_to_str
from scrapy import log
from hubstorage import Client

# this is to provide access to logger stats (ie. number of errors) from
# external modules
observer = None

def initialize_hubstorage_logging():
    """Initialized twisted logging to use only a hubstorage log observer

    Scrapy will no longer initialize logging and nothing will be send to stdout
    or stderr.
    """
    global observer

    level = getattr(log, settings['LOG_LEVEL'])
    url = settings.get('HUBSTORAGE_URL', 'http://localhost:8002')
    e = os.environ
    observer = HubStorageLogObserver(e['SHUB_JOBAUTH'], e['SHUB_PROJECT'],
        e['SHUB_SPIDER'], e['SHUB_JOB'], level=level, url=url)
    txlog.startLoggingWithObserver(observer.emit, setStdout=False)
    from twisted.internet import reactor
    reactor.addSystemEventTrigger('after', 'shutdown', observer.stop)
    logfile = settings['LOG_FILE']
    if logfile:
        sflo = log.ScrapyFileLogObserver(open(logfile, 'w'), level, 'utf-8')
        txlog.addObserver(sflo.emit)

def get_log_item(ev, min_level=log.INFO):
    """Get HubStorage log item for the given Twisted event, or None if no
    document should be inserted
    """
    if ev['system'] == 'scrapy':
        level = ev['logLevel']
    else:
        if ev['isError']:
            level = log.ERROR
        else:
            return # ignore non-scrapy & non-error messages
    if level < min_level:
        return
    msg = ev.get('message')
    if msg:
        msg = unicode_to_str(msg[0])
    failure = ev.get('failure', None)
    if failure:
        msg = failure.getTraceback()
    why = ev.get('why', None)
    if why:
        msg = "%s\n%s" % (why, msg)
    msg = msg.replace('\n', '\n\t') # to replicate typical scrapy log appeareance
    return {'message': msg, 'level': level, 'time': int(time.time()*1000)}

class HubStorageLogObserver(object):

    def __init__(self, apikey, project, spider, job, level=log.INFO, url='http://localhost:8002'):
        self.level = level
        self.errors_count = 0
        self.client = Client(apikey, url=url)
        path = "/logs/%s/%s/%s" % (project, spider, job)
        self.writer = self.client.open_item_writer(path)

    def emit(self, ev):
        logitem = get_log_item(ev, self.level)
        if logitem:
            self.writer.write_item(logitem)
            self.errors_count += logitem['level'] == log.ERROR

    def stop(self):
        self.writer.close()
