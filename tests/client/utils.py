from .conftest import TEST_PROJECT_ID, TEST_SPIDER_NAME
from .conftest import TEST_DASH_ENDPOINT


def validate_default_meta(meta, state='pending', units=1,
                          priority=2, tags=None):
    assert meta.get('project') == int(TEST_PROJECT_ID)
    assert meta.get('spider') == TEST_SPIDER_NAME
    assert meta.get('state') == state
    assert meta.get('priority') == priority
    assert meta.get('spider_type') == 'manual'
    assert meta.get('tags') == (tags or [])
    assert meta.get('units') == units
    assert meta.get('api_url') == TEST_DASH_ENDPOINT
    assert meta.get('portia_url')
