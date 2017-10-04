""" Serialization utils module.  """

from datetime import datetime, timedelta, tzinfo

from scrapinghub.hubstorage.serialization import jsondefault


def test_jsondefault_timezones():

    class TestTZ(tzinfo):

        def utcoffset(self, dt):
            return timedelta(minutes=-399)

    # base tz unaware datetime
    dt = datetime(2017, 5, 4, 3, 2, 1, 123456)
    dt_ts = jsondefault(dt)
    # tz aware datetime with test TZ
    dt_tz = dt.replace(tzinfo=TestTZ())
    dt_tz_ts = jsondefault(dt_tz)
    assert dt_tz_ts == dt_ts + 399 * 60 * 1000
