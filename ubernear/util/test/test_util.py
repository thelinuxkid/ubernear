from nose.tools import eq_ as eq
from dateutil import tz
from datetime import datetime

from ubernear import util
from ubernear.test.util import assert_raises

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

def test_utc_from_iso_aware():
    dt = util.utc_from_iso8601('2011-11-16T18:36:06.795119-08:00')
    eq(dt, datetime(2011, 11, 17, 2, 36, 06, 795119, tz.tzutc()))

def test_utc_from_iso_aware_utc():
    dt = util.utc_from_iso8601('2011-10-12T19:55:58.345128+0000')
    eq(dt, datetime(2011, 10, 12, 19, 55, 58, 345128, tz.tzutc()))

def test_utc_from_iso_to_naive():
    dt = util.utc_from_iso8601(
        '2011-11-16T18:36:06.795119-08:00',
        naive=True,
    )
    eq(dt, datetime(2011, 11, 17, 2, 36, 06, 795119))

def test_utc_from_iso_to_naive_utc():
    dt = util.utc_from_iso8601(
        '2011-10-12T19:55:58.345128+0000',
        naive=True,
    )
    eq(dt, datetime(2011, 10, 12, 19, 55, 58, 345128))

def test_utc_from_empty():
    msg = assert_raises(
        ValueError,
        util.utc_from_iso8601,
        '',
        )

    eq(str(msg), 'string cannot be empty')

def test_utc_from_iso_no_timezone():
    msg = assert_raises(
        ValueError,
        util.utc_from_iso8601,
        '2011-11-17T2:30:58.403397',
    )

    eq(str(msg), 'string must contain timezome information')

def test_utc_from_iso_from_naive():
    msg = assert_raises(
        ValueError,
        util.utc_from_iso8601,
        '2011-11-T2:30:58.403397',
    )

    eq(str(msg), 'unknown string format')

def test_utc_to_local_simple():
    dt = util.utc_to_local(
        datetime(2011, 11, 17, 2, 36, 06, 795119, tz.tzutc())
    )
    eq(dt,
       datetime(2011, 11, 16, 18, 36, 06, 795119, tz.tzlocal())
    )

def test_utc_to_local_to_naive():
    dt = util.utc_to_local(
        datetime(2011, 11, 17, 2, 36, 06, 795119, tz.tzutc()),
        naive=True,
    )
    eq(dt,
       datetime(2011, 11, 16, 18, 36, 06, 795119)
    )

def test_utc_to_local_from_naive():
    dt = datetime(2011, 10, 12, 2, 30, 58, 403397)
    msg = assert_raises(
        ValueError,
        util.utc_to_local,
        dt,
    )

    eq(str(msg), 'Datetime is not in UTC')

def test_utc_to_local_not_in_utc():
    dt = datetime(2011, 10, 12, 2, 30, 58, 403397, tz.tzlocal())
    msg = assert_raises(
        ValueError,
        util.utc_to_local,
        dt,
    )

    eq(str(msg), 'Datetime is not in UTC')
