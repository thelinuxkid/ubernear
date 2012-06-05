import fudge

from nose.tools import eq_ as eq
from datetime import datetime, tzinfo

from ubernear import util

def test_takeslice_simple():
    data = range(1,4+1)

    res = util.takeslice(data)

    expected = [
        [1],
        [2],
        [3],
        [4],
        ]
    eq(list(res), expected)

def test_takeslice_step():
    data = range(1,60+1)

    res = util.takeslice(data, step=20)

    expected = [
        [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],
        [21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40],
        [41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60],
        ]
    eq(list(res), expected)

def test_takeslice_iter_not_exact():
    data = range(1,42+1)

    res = util.takeslice(data, step=20)

    expected = [
        [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],
        [21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40],
        [41,42],
        ]
    eq(list(res), expected)

def test_takeslice_iter_smaller_than_step():
    data = range(1,13+1)

    res = util.takeslice(data, step=20)

    expected = [
        [1,2,3,4,5,6,7,8,9,10,11,12,13],
        ]
    eq(list(res), expected)

def test_utc_from_iso_naive():
    dt = util.utc_from_iso8601('2011-11-17T2:30:58.403397')
    eq(dt, datetime(2011, 11, 17, 2, 30, 58, 403397))
    eq(dt.tzinfo, None)

def test_utc_from_iso_aware():
    dt = util.utc_from_iso8601('2011-11-16T18:36:06.795119-08:00')
    eq(dt, datetime(2011, 11, 17, 2, 36, 06, 795119))
    eq(dt.tzinfo, None)

def test_utc_from_iso_aware_utc():
    dt = util.utc_from_iso8601('2011-10-12T19:55:58+0000')
    eq(dt, datetime(2011, 10, 12, 19, 55, 58))
    eq(dt.tzinfo, None)

def test_utc_from_iso_half_aware():
    fake_parser = fudge.Fake('parser')
    fake_parser.remember_order()

    fake_parser.expects('parse')
    fake_parser.with_args('2011-10-12T19:55:58+0000')
    class fake_tzinfo(tzinfo):
        def utcoffset(self, dt):
            return None
    dt = datetime(2011, 10, 12, 19, 55, 58, tzinfo=fake_tzinfo())
    fake_parser.returns(dt)

    dt = util.utc_from_iso8601(
        '2011-10-12T19:55:58+0000',
        _parser=fake_parser,
        )
    eq(dt, datetime(2011, 10, 12, 19, 55, 58))
    eq(dt.tzinfo, None)
