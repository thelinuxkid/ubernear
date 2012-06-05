import hashlib
import json
import logging
import bottle
import pymongo
import functools

from paste import httpserver
from paste.translogger import TransLogger
from datetime import datetime
from collections import OrderedDict, defaultdict

from ubernear import util

log = logging.getLogger(__name__)

facebook_events_url = 'http://facebook.com/events'
api_version = '0.1'

class DefaultOrderedDict(defaultdict, OrderedDict):
    def __init__(self, default_factory):
        defaultdict.__init__(self, default_factory)
        OrderedDict.__init__(self)

class APILogger(TransLogger):
    def write_log(
        self,
        environ,
        method,
        req_uri,
        start,
        status,
        bytes_,
        ):
        remote_addr = environ['REMOTE_ADDR']
        protocol = environ['SERVER_PROTOCOL']
        referer = environ.get('HTTP_REFERER', '-')
        user_agent = environ.get('HTTP_USER_AGENT', '-')
        msg = ('{remote_addr} {method} {req_uri} {protocol} {status} '
               '{bytes_} {referer} {user_agent}'
               ).format(
            remote_addr=remote_addr,
            method=method,
            req_uri=req_uri,
            protocol=protocol,
            status=status,
            bytes_=bytes_,
            referer=referer,
            user_agent=user_agent,
            )
        log.info(msg)

class APIServer(bottle.ServerAdapter):
    def run(self, handler):
        handler = APILogger(handler)
        httpserver.serve(
            handler,
            host=self.host,
            port=str(self.port),
            **self.options
            )

class APIHTTPError(bottle.HTTPError):
    def __repr__(self):
        return self.output

def get_status(
    code=200,
    message='OK',
    ):
    status = OrderedDict([
            ('code', code),
            ('message', message)
            ])
    res = OrderedDict([
            ('status', status),
            ])

    return res

def _error(error):
    bottle.response.content_type = 'application/json'
    status = get_status(
        code=error.status,
        message=error.output,
        )

    return json.dumps(status)

@bottle.error(500)
def error_default(error):
    return _error(error)

@bottle.error(404)
def error_404(error):
    try:
        output = json.loads(error.output)
        error.output = output['status']['message']
    except (ValueError, KeyError):
        pass

    return _error(error)

def send_error(code, message):
    status = get_status(
        code=code,
        message=message,
        )
    status = json.dumps(status)

    raise APIHTTPError(
        code=code,
        output=status,
        )

def check_version(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if kwargs['version'] != api_version:
            send_error(
                code=404,
                message='Invalid API version',
                )
        return fn(*args, **kwargs)

    return wrapper

class EventAPI01(object):
    def __init__(
        self,
        keys_coll,
        events_coll,
        ):
        self._keys_coll = keys_coll
        self._events_coll = events_coll

    def apply(self, callback, context):
        """
        Similar to a bottle.JSONPlugin's apply
        method. This one also ensures that self
        is available to methods with bottle
        decorators.
        """
        def wrapper(*args, **kwargs):
            kwargs['self'] = self
            bottle.response.content_type = 'application/json'
            return callback(*args, **kwargs)
        return wrapper

    def _check_and_get_key(
        self,
        _request=None,
        ):
        if _request is None:
            _request = bottle.request

        query = _request.query
        key = query.key
        # request.query returns empty if attr doesn't exist
        if not key:
            send_error(
                code=400,
                message='You must specify an API key',
                )

        message = 'Invalid API key'
        host = _request.environ.get('REMOTE_ADDR')
        db_key = self._keys_coll.find_one(host)
        if db_key is None:
            send_error(
                code=400,
                message=message,
                )

        if db_key['disabled'] is True:
            send_error(
                code=400,
                message=message,
                )

        _hash = hashlib.sha256()
        _hash.update(host)
        _hash.update(db_key['secret'])
        _hash = _hash.hexdigest()

        if key != _hash:
            send_error(
                code=400,
                message=message,
                )

        return host

    def _check_and_get_until(
        self,
        _request=None,
        ):
        if _request is None:
            _request = bottle.request

        until = _request.query.until
        if until != '':
            try:
                until = util.utc_from_iso8601(until)
                until = util.utc_to_local(until, naive=True)
            except ValueError:
                send_error(
                    code=400,
                    message='Invalid until parameter value',
                    )
            else:
                query = OrderedDict([
                        ('facebook.start_time', OrderedDict([
                                    ('$lte', until),
                                    ]),
                         ),
                        ])
                return query

        return None

    def _update_key(
        self,
        key,
        now,
        ):
        self._keys_coll.update(
            OrderedDict([
                    ('_id', key),
                    ]),
            OrderedDict([
                    ('$set', OrderedDict([
                                ('last_used', now),
                                ])
                     ),
                    ('$inc', OrderedDict([
                                ('times_used', 1),
                                ])
                     ),
                    ]),
            )

    def _get_results_by_coord(
        self,
        events,
        now,
        details=False,
        ):
        grouped = DefaultOrderedDict(list)
        places = OrderedDict()
        event_count = 0
        for event in events:
            facebook = event['facebook']
            if facebook['end_time'] < now:
                continue

            place = event['match']['place']
            place_id = event['match']['ubernear']['place_id']
            name = place['name']

            # Events could have the same _id but different
            # names since the name stored was the one given
            # by facebook
            api_place_id = hashlib.md5()
            api_place_id.update(place_id)
            api_place_id.update(name.encode('utf-8'))
            api_place_id = api_place_id.hexdigest()
            places[api_place_id] = place

            link = '{facebook_events_url}/{_id}'.format(
                facebook_events_url=facebook_events_url,
                _id=facebook['id'],
                )

            api_event = OrderedDict([
                    ('id', facebook['id']),
                    ])
            if details is True:
                api_event['event_link'] = link
                api_event['name'] = facebook['name']
                api_event['start_time'] = facebook['start_time']
                api_event['end_time'] = facebook['end_time']
                api_event['place_id'] = api_place_id

                description = facebook.get('description', None)
                if description:
                    api_event['description'] = description

            loc = event['match']['ubernear']['location']
            (lng, lat) = loc
            # Does not lose precision
            lat = json.dumps(lat)
            lng = json.dumps(lng)

            api_loc = '{lat},{lng}'.format(lat=lat, lng=lng)
            grouped[api_loc].append(api_event)
            event_count += 1

        status = get_status()
        count = OrderedDict([
                ('events', event_count),
                ('places', len(places)),
                ('coordinates', len(grouped.keys())),
                ])
        data = OrderedDict([
                ('events', grouped),
                ])
        if details is True:
            data['places'] = places

        status['count'] = count
        status['data'] = data

        results = json.dumps(
            status,
            default=datetime.isoformat,
            )

        return results

    def _all(
        self,
        _datetime=None,
        _request=None,
        ):
        if _datetime is None:
            _datetime = datetime

        key = self._check_and_get_key(_request=_request)
        # TODO. Use Los Angeles local time until the timezone
        # is included in each event's data
        now = _datetime.now()

        # Allow mongodb to cache requests for today
        today = now.replace(hour=0,minute=0,second=0,microsecond=0)

        match = OrderedDict([
                ('match', OrderedDict([
                            ('$exists', True),
                            ]),
                 ),
                ])
        end_time = OrderedDict([
                ('facebook.end_time', OrderedDict([
                            ('$gt', today),
                            ]),
                 ),
                ])
        and_parts = [match, end_time]

        start_time = self._check_and_get_until(_request=_request)
        if start_time is not None:
            and_parts.append(start_time)

        events = self._events_coll.find(
            OrderedDict([
                    ('$and', and_parts)
                    ]),
            sort=[('facebook.start_time', pymongo.ASCENDING)],
            )

        results = self._get_results_by_coord(
            events=events,
            now=now,
            )
        self._update_key(
            key=key,
            now=now,
            )

        return results

    def _single_coord(
        self,
        lat,
        lng,
        _datetime=None,
        _request=None,
        ):
        if _datetime is None:
            _datetime = datetime

        try:
            lng = float(lng)
            lat = float(lat)
        except ValueError:
            send_error(
                code=400,
                message='Invalid coordinates',
                )

        key = self._check_and_get_key(_request=_request)
        location = OrderedDict([
                ('match.ubernear.location', [lng,lat])
                ])
        and_parts = [location]

        start_time = self._check_and_get_until(_request=_request)
        if start_time is not None:
            and_parts.append(start_time)

        events = self._events_coll.find(
            OrderedDict([
                    ('$and', and_parts)
                    ]),
            sort=[('facebook.start_time', pymongo.ASCENDING)],
            )

        # TODO. Use Los Angeles local time until the timezone
        # is included in each event's data
        now = _datetime.now()
        results = self._get_results_by_coord(
            events=events,
            now=now,
            details=True,
            )
        self._update_key(
            key=key,
            now=now,
            )

        return results

    def _no_version(self):
        send_error(
            code=404,
            message='You must specify an API version',
            )

    @bottle.get('/<version>')
    @bottle.get('/<version>/')
    @check_version
    def all(self, version):
        return self._all()

    @bottle.get('/<version>/<lat:float>,<lng:float>')
    @bottle.get('/<version>/<lat:float>,<lng:float>/')
    @check_version
    def single_coord(self, version, lat, lng):
        return self._single_coord(lat, lng)

    @bottle.get('/')
    def no_version(self):
        self._no_version()
