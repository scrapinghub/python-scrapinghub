from collections import Iterator

from scrapinghub.client.items import Items
from scrapinghub.client.jobs import Job
from scrapinghub.client.jobs import JobMeta
from scrapinghub.client.logs import Logs
from scrapinghub.client.requests import Requests
from scrapinghub.client.samples import Samples

from .conftest import TEST_PROJECT_ID
from .conftest import TEST_SPIDER_NAME


def test_job_base(client, spider):
    job = spider.jobs.schedule()
    assert isinstance(job, Job)
    assert job.projectid == TEST_PROJECT_ID
    assert job.key.startswith(spider.key)

    assert isinstance(job.items, Items)
    assert isinstance(job.logs, Logs)
    assert isinstance(job.requests, Requests)
    assert isinstance(job.samples, Samples)
    assert isinstance(job.metadata, JobMeta)


def test_job_update_tags(spider):
    job1 = spider.jobs.schedule(spider_args={'subid': 'tags-1'},
                                add_tag=['tag1'])
    job2 = spider.jobs.schedule(spider_args={'subid': 'tags-2'},
                                add_tag=['tag2'])
    # FIXME the endpoint normalises tags so it's impossible to send tags
    # having upper-cased symbols, let's add more tests when it's fixed
    assert job1.update_tags(add=['tag11', 'tag12']) == 1
    assert job1.metadata.get('tags') == ['tag1', 'tag11', 'tag12']

    assert job1.update_tags(remove=['tag1', 'tagx']) == 1
    assert job1.metadata.get('tags') == ['tag11', 'tag12']

    # assert that 2nd job tags weren't changed
    assert job2.metadata.get('tags') == ['tag2']
    # FIXME adding and removing tags at the same time doesn't work neither:
    # remove-tag field is ignored if there's non-void add-tag field


def test_job_start(spider):
    job = spider.jobs.schedule()
    assert job.metadata.get('state') == 'pending'
    job.start()
    assert job.metadata.get('state') == 'running'
    assert isinstance(job.metadata.get('pending_time'), int)
    assert isinstance(job.metadata.get('running_time'), int)
    assert job.metadata.get('spider') == TEST_SPIDER_NAME
    assert job.metadata.get('priority') == 2


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
    assert job.start(**extras) == 'pending'
    for k, v in extras.items():
        if type(v) is float:
            assert abs(job.metadata.get(k) - v) < 0.0001
        else:
            assert job.metadata.get(k) == v


def test_job_update(spider):
    job = spider.jobs.schedule()
    assert job.metadata.get('state') == 'pending'
    job.update(state='running', foo='bar')

    job = spider.jobs.get(job.key)
    assert job.metadata.get('state') == 'running'
    assert job.metadata.get('foo') == 'bar'


def test_job_cancel_pending(spider):
    job = spider.jobs.schedule()
    assert job.metadata.get('state') == 'pending'
    job.cancel()
    assert job.metadata.get('state') == 'finished'


def test_job_cancel_running(spider):
    job = spider.jobs.schedule()
    job.start()
    assert job.metadata.get('state') == 'running'
    job.cancel()
    # still running as should be stopped by scheduler
    assert job.metadata.get('state') == 'running'


def test_job_finish(spider):
    job = spider.jobs.schedule()
    assert job.metadata.get('state') == 'pending'
    job.finish()
    assert job.metadata.get('state') == 'finished'


def test_job_finish_with_metadata(spider):
    job = spider.jobs.schedule(meta={'meta1': 'val1', 'meta2': 'val3'})
    assert job.metadata.get('state') == 'pending'
    job.finish(meta2='val2', meta3='val3')
    assert job.metadata.get('state') == 'finished'
    assert job.metadata.get('meta1') == 'val1'
    assert job.metadata.get('meta2') == 'val2'
    assert job.metadata.get('meta3') == 'val3'


def test_job_delete(spider):
    job = spider.jobs.schedule(meta={'state': 'finished'})
    assert job.metadata.get('state') == 'finished'
    job.delete()
    assert job.metadata.get('state') == 'deleted'


def test_job_delete_with_metadata(spider):
    meta = {'state': 'finished', 'meta1': 'val1', 'meta2': 'val3'}
    job = spider.jobs.schedule(meta=meta)
    assert job.metadata.get('state') == 'finished'
    job.delete(meta2='val2', meta3='val3')
    assert job.metadata.get('state') == 'deleted'
    assert job.metadata.get('meta1') == 'val1'
    assert job.metadata.get('meta2') == 'val2'
    assert job.metadata.get('meta3') == 'val3'


def test_metadata_update(spider):
    job = spider.jobs.schedule(meta={'meta1': 'data1'})
    assert job.metadata.get('meta1') == 'data1'
    job.metadata.update({'meta1': 'data2', 'meta2': 'data3'})
    assert job.metadata.get('meta1') == 'data2'
    assert job.metadata.get('meta2') == 'data3'


def test_metadata_set(spider):
    job = spider.jobs.schedule(meta={'meta1': 'data1'})
    assert job.metadata.get('meta1') == 'data1'
    job.metadata.set('meta1', 'data2')
    job.metadata.set('meta2', 123)
    assert job.metadata.get('meta1') == 'data2'
    assert job.metadata.get('meta2') == 123


def test_metadata_delete(spider):
    job = spider.jobs.schedule(meta={'meta1': 'data1', 'meta2': 'data2'})
    job.metadata.delete('meta1')
    assert job.metadata.get('meta1') is None
    assert job.metadata.get('meta2') == 'data2'


def test_metadata_iter_list(spider):
    job = spider.jobs.schedule(meta={'meta1': 'data1', 'meta2': 'data2'})
    meta_iter = job.metadata.iter()
    assert isinstance(meta_iter, Iterator)
    meta_list = job.metadata.list()
    assert ('meta1', 'data1') in meta_list
    assert ('meta2', 'data2') in meta_list
    assert meta_list == list(meta_iter)
