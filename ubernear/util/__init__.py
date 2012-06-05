import dateutil.parser
import dateutil.tz
import json
import signal
import sys
import logging

from datetime import datetime
from collections import OrderedDict, defaultdict

log = logging.getLogger(__name__)

def read_http(res):
    while True:
        data = res.read(4*1024*1024)
        if not data:
            break
        yield data

def utc_from_iso8601(
    dt_str,
    _parser=None,
    ):
    if _parser is None:
        _parser = dateutil.parser

    # dateutil.parser.parse returns today's date when fed the empty string
    if dt_str == '':
        raise ValueError
    dt = _parser.parse(dt_str)
    if dt.tzinfo is not None:
        if dt.tzinfo.utcoffset(dt) is not None:
            dt = dt.astimezone(dateutil.tz.tzutc())
        dt = dt.replace(tzinfo=None)

    return dt


def read_json(res):
    data = [datum for datum in read_http(res)]
    data = ''.join(data)
    data = json.loads(data)

    return data

def signal_handler(func):
    def handler(signum, frame):
        msg = 'Unknown'
        if signum == signal.SIGTERM:
            msg = 'Terminated'
        elif signum == signal.SIGCONT:
            msg = 'Continued'
        log.warn(
            '{msg}'.format(
                msg=msg,
                )
            )
        sys.exit(1)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGCONT, handler)

    return func

def takeslice(iterable, step=1):
    last_i = 0
    for i in xrange(0, len(iterable), step):
        i += step
        yield iterable[last_i:i]
        last_i = i

def print_odict(src):
    def iso_format(dt):
        if isinstance(dt, datetime):
            return dt.isoformat()

    def _print_odict(odict):
        gen = _print_odict_rec(odict)
        if gen is not None:
            print 'OrderedDict(['
            for pair in gen:
                print str(pair) + ','
            print '])'

    def _print_odict_rec(odict):
        if isinstance(odict, dict):
            for k,v in odict.iteritems():
                if isinstance(v, dict):
                    print "('%s', OrderedDict([" % k
                    pair = _print_odict_rec(v)
                    for p in pair:
                        if p is not None:
                            print str(p) + ','
                    print ']),'
                    print '),'
                else:
                    if isinstance(v, unicode):
                        v = str(v)
                        try:
                            datetime.strptime(
                                v,
                                '%Y-%m-%dT%H:%M:%S.%f',
                                )
                        except ValueError:
                            pass
                    yield str(k), v
        else:
            yield None

    dumped = json.dumps(src, default=iso_format)
    loaded = json.loads(dumped, object_hook=OrderedDict)
    _print_odict(loaded)


class DefaultOrderedDict(defaultdict, OrderedDict):
    def __init__(self, default_factory):
        defaultdict.__init__(self, default_factory)
        OrderedDict.__init__(self)
