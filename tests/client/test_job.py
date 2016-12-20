import six

from scrapinghub.client import Job
from scrapinghub.client import Items, Logs, Requests
from scrapinghub.client import Samples, JobMeta

from .conftest import TEST_PROJECT_ID
from .conftest import TEST_SPIDER_NAME


def test_job_base(client, spider):
    job = spider.jobs.schedule()
    assert isinstance(job, Job)
    assert job.projectid == int(TEST_PROJECT_ID)
    assert job.key.startswith(TEST_PROJECT_ID + '/' + str(spider.id))

    assert isinstance(job.items, Items)
    assert isinstance(job.logs, Logs)
    assert isinstance(job.requests, Requests)
    assert isinstance(job.samples, Samples)
    assert isinstance(job.metadata, JobMeta)


def test_job_update_metadata(spider):
    job = spider.jobs.schedule(meta={'meta1': 'data1'})
    assert job.metadata['meta1'] == 'data1'
    job.update_metadata(meta1='data2', meta2='data3')
    job.metadata.expire()
    assert job.metadata['meta1'] == 'data2'
    assert job.metadata['meta2'] == 'data3'


def test_job_update_tags(spider):
    job1 = spider.jobs.schedule(subid='tags-1', add_tag=['tag1'])
    job2 = spider.jobs.schedule(subid='tags-2', add_tag=['tag2'])

    # FIXME the endpoint normalises tags so it's impossible to send tags
    # having upper-cased symbols, let's add more tests when it's fixed
    assert job1.update_tags(add=['tag11', 'tag12']) == 1
    assert job1.metadata.liveget('tags') == ['tag1', 'tag11', 'tag12']

    assert job1.update_tags(remove=['tag1', 'tagx']) == 1
    assert job1.metadata.liveget('tags') == ['tag11', 'tag12']

    # assert that 2nd job tags weren't changed
    assert job2.metadata.liveget('tags') == ['tag2']
    # FIXME adding and removing tags at the same time doesn't work neither:
    # remove-tag field is ignored if there's non-void add-tag field


def test_job_start(spider):
    job = spider.jobs.schedule()
    assert job.metadata['state'] == 'pending'
    job.start()
    job.metadata.expire()
    assert job.metadata['state'] == 'running'
    assert isinstance(job.metadata['pending_time'], int)
    assert isinstance(job.metadata['running_time'], int)
    assert job.metadata['spider'] == TEST_SPIDER_NAME
    assert job.metadata['priority'] == 2


def test_job_start_extras(spider):
    job = spider.jobs.schedule()
    extras = {
        'string': 'foo',
        'integer': 1,
        'float': 3.2,
        'mixedarray': ['b', 1, None, True, False, {'k': 'c'}],
        'emptyarray': [],
        'mapping': {'alpha': 5, 'b': 'B', 'cama': []},
        'emptymapping': {},
        'true': True,
        'false': False,
        'nil': None,
    }
    started = next(job.start(**extras))
    assert job.key == started['key']
    for k, v in six.iteritems(extras):
        if type(v) is float:
            assert abs(job.metadata.get(k) - v) < 0.0001
        else:
            assert job.metadata.get(k) == v


def test_job_update(spider):
    job = spider.jobs.schedule()
    assert job.metadata['state'] == 'pending'
    job.update(state='running', foo='bar')

    job = spider.jobs.get(job.key)
    assert job.metadata['state'] == 'running'
    assert job.metadata['foo'] == 'bar'


def test_job_cancel_pending(spider):
    job = spider.jobs.schedule()
    assert job.metadata['state'] == 'pending'
    job.metadata.expire()
    job.cancel()
    assert job.metadata['state'] == 'finished'


def test_job_cancel_running(spider):
    job = spider.jobs.schedule()
    job.start()
    assert job.metadata['state'] == 'running'
    job.metadata.expire()
    job.cancel()
    # still running as should be stopped by scheduler
    assert job.metadata['state'] == 'running'


def test_job_finish(spider):
    job = spider.jobs.schedule()
    assert job.metadata['state'] == 'pending'
    job.metadata.expire()
    job.finish()
    assert job.metadata.get('state') == 'finished'


def test_job_finish_with_metadata(spider):
    job = spider.jobs.schedule(meta={'meta1': 'val1', 'meta2': 'val3'})
    assert job.metadata['state'] == 'pending'
    job.metadata.expire()
    job.finish(meta2='val2', meta3='val3')
    assert job.metadata.get('state') == 'finished'
    assert job.metadata['meta1'] == 'val1'
    assert job.metadata['meta2'] == 'val2'
    assert job.metadata['meta3'] == 'val3'


def test_job_delete(spider):
    job = spider.jobs.schedule(meta={'state': 'finished'})
    assert job.metadata['state'] == 'finished'
    job.metadata.expire()
    job.delete()
    assert job.metadata.get('state') == 'deleted'


def test_job_delete_with_metadata(spider):
    meta = {'state': 'finished', 'meta1': 'val1', 'meta2': 'val3'}
    job = spider.jobs.schedule(meta=meta)
    assert job.metadata['state'] == 'finished'
    job.metadata.expire()
    job.delete(meta2='val2', meta3='val3')
    assert job.metadata.get('state') == 'deleted'
    assert job.metadata['meta1'] == 'val1'
    assert job.metadata['meta2'] == 'val2'
    assert job.metadata['meta3'] == 'val3'


def test_job_purge(spider):
    meta = {'state': 'finished', 'meta1': 'val1'}
    job = spider.jobs.schedule(meta=meta)
    assert job.metadata['state'] == 'finished'
    job.purge()
    assert job.metadata['state'] == 'deleted'
    assert job.metadata['meta1'] == 'val1'


def test_job_close_writers(spider):
    job = spider.jobs.schedule()
    job.close_writers()

    job.logs.info('test-log')
    job.items.write({'item_data': 'value'})
    job.requests.add('some-url', 200, 'GET', 0, None, 10, 0)
    job.samples.write([1, 2, 3, 4])
    job.close_writers()
