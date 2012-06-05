import json
import fudge

from nose.tools import eq_ as eq
from collections import OrderedDict
from datetime import datetime

from ubernear.test.util import assert_raises
from ubernear.api import (
    EventAPI01,
    APIHTTPError,
    get_status,
    send_error,
    check_version,
    )

class TestApi(object):
    def setUp(self):
        fudge.clear_expectations()

    @fudge.with_fakes
    def test_get_status_simple(self):
        res = get_status(
            code=400,
            message='FOO ERROR',
            )

        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 400),
                            ('message', 'FOO ERROR'),
                            ]),
                 ),
                ])
        eq(res, expected)

    @fudge.with_fakes
    def test_send_error_simple(self):
        error = assert_raises(
            APIHTTPError,
            send_error,
            code=404,
            message='FOO ERROR',
            )

        eq(
            error.output,
            '{"status": {"code": 404, "message": "FOO ERROR"}}',
            )
        eq(error.status, 404)

    @fudge.with_fakes
    def test_check_version_simple(self):
        fake_res = fudge.Fake('res')
        fake_api_call = fudge.Fake('foo_api_call', callable=True)
        fake_api_call.times_called(1)
        fake_api_call.with_args(version='0.1')
        name = {'__name__': 'foo_api_call'}
        fake_api_call.has_attr(**name)
        fake_api_call.returns(fake_res)

        wrapper = check_version(fake_api_call)
        res = wrapper(version='0.1')

        eq(res, fake_res)

    @fudge.with_fakes
    def test_check_version_error(self):
        fake_res = fudge.Fake('res')
        fake_api_call = fudge.Fake('foo_api_call', callable=True)
        fake_api_call.times_called(1)
        fake_api_call.with_args(version='0.1')
        name = {'__name__': 'foo_api_call'}
        fake_api_call.has_attr(**name)
        fake_api_call.returns(fake_res)

        wrapper = check_version(fake_api_call)
        error = assert_raises(
            APIHTTPError,
            wrapper,
            version='foo',
            )

        eq(
            error.output,
            '{"status": {"code": 404, "message": '
            '"Invalid API version"}}'
            )
        eq(error.status, 404)

    @fudge.with_fakes
    def test_check_and_get_key_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        key = api._check_and_get_key(
            _request=fake_request,
            )

        eq(key, 'foo host')

    @fudge.with_fakes
    def test_check_and_get_key_missing(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        query.has_attr(key=None)
        fake_request.has_attr(query=query)

        error = assert_raises(
            APIHTTPError,
            api._check_and_get_key,
            _request=fake_request,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"You must specify an API key"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_check_and_get_key_not_assigned(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        find_one.returns(None)

        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        error = assert_raises(
            APIHTTPError,
            api._check_and_get_key,
            _request=fake_request,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid API key"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_check_and_get_key_disabled(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', True),
                ])
        find_one.returns(key)

        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        query.has_attr(key='foo hash')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        error = assert_raises(
            APIHTTPError,
            api._check_and_get_key,
            _request=fake_request,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid API key"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_check_and_get_key_invalid(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        query.has_attr(key='foo hash')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        error = assert_raises(
            APIHTTPError,
            api._check_and_get_key,
            _request=fake_request,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid API key"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_check_and_get_until_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        query.has_attr(until='2012-02-11T05:55:43.965992+00:00')
        fake_request.has_attr(query=query)

        res = api._check_and_get_until(
            _request=fake_request,
            )

        eq(res,
           OrderedDict([
                    ('facebook.start_time', OrderedDict([
                                ('$lte',
                                 datetime(2012, 2, 10, 21, 55, 43, 965992),
                                 ),
                                ]),
                     ),
                    ])
           )

    @fudge.with_fakes
    def test_check_and_get_until_error(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        query.has_attr(until='foo')
        fake_request.has_attr(query=query)

        error = assert_raises(
            APIHTTPError,
            api._check_and_get_until,
            _request=fake_request,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid until parameter value"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_check_and_get_until_none(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        query.has_attr(until='')
        fake_request.has_attr(query=query)

        res = api._check_and_get_until(
            _request=fake_request,
            )

        eq(res, None)

    @fudge.with_fakes
    def test_no_version_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        error = assert_raises(
            APIHTTPError,
            api._no_version,
            )

        eq(
            error.output,
            '{"status": {"code": 404, "message": "You must '
            'specify an API version"}}',
            )
        eq(error.status, 404)

    @fudge.with_fakes
    def test_update_key_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        update = keys_coll.expects('update')
        now = datetime(2012, 1, 23, 5, 26, 56)
        change = OrderedDict([
                ('$set', OrderedDict([
                            ('last_used', now),
                            ]),
                 ),
                ('$inc', OrderedDict([
                            ('times_used', 1),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([('_id', 'foo')]),
            change,
            )

        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        api._update_key(
            key='foo',
            now=now,
            )

    @fudge.with_fakes
    def test_get_results_by_coord_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_details(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_description(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('description', 'My Birthday Party'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                ('description', 'My Birthday Party'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_same_place(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event_1 = OrderedDict([
                ('_id', '267649766610622'),
                ('facebook', OrderedDict([
                            ('name', 'Bachelor Party'),
                            ('id', '267649766610622'),
                            ('start_time',
                             datetime(2012, 2, 12, 3, 30),
                             ),
                            ('end_time',
                             datetime(2012, 2, 12, 20),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])
        event_2 = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event_1, event_2],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event_1 = OrderedDict([
                ('id', '267649766610622'),
                ('event_link', 'http://facebook.com/events/267649766610622'),
                ('name', 'Bachelor Party'),
                ('start_time',
                 datetime(2012, 2, 12, 3, 30),
                 ),
                ('end_time',
                 datetime(2012, 2, 12, 20)
                 ),
                 ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                 ])
        res_event_2 = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 2),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event_1,
                                                res_event_2,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_same_place_different_name(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place_1 = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        place_2 = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'HousePlay'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event_1 = OrderedDict([
                ('_id', '267649766610622'),
                ('facebook', OrderedDict([
                            ('name', 'Bachelor Party'),
                            ('id', '267649766610622'),
                            ('start_time',
                             datetime(2012, 2, 12, 3, 30),
                             ),
                            ('end_time',
                             datetime(2012, 2, 12, 20),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_1),
                            ])
                 ),
                ])
        event_2 = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_2),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event_1, event_2],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event_1 = OrderedDict([
                ('id', '267649766610622'),
                ('event_link', 'http://facebook.com/events/267649766610622'),
                ('name', 'Bachelor Party'),
                ('start_time',
                 datetime(2012, 2, 12, 3, 30),
                 ),
                ('end_time',
                 datetime(2012, 2, 12, 20)
                 ),
                 ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                 ])
        res_event_2 = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', '3f1743763812cec5508eef532ef7fedc'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 2),
                            ('places', 2),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event_1,
                                                res_event_2,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place_1,
                                         ),
                                        ('3f1743763812cec5508eef532ef7fedc',
                                         place_2,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_same_coord_different_place(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place_1 = OrderedDict([
                ('address', '6555 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Tabaco House'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        place_2 = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'HousePlay'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event_1 = OrderedDict([
                ('_id', '267649766610622'),
                ('facebook', OrderedDict([
                            ('name', 'Bachelor Party'),
                            ('id', '267649766610622'),
                            ('start_time',
                             datetime(2012, 2, 12, 3, 30),
                             ),
                            ('end_time',
                             datetime(2012, 2, 12, 20),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'ffd3b102-14c1-49dc-9bf2-6cb040a10bba'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_1),
                            ])
                 ),
                ])
        event_2 = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_2),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event_1, event_2],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event_1 = OrderedDict([
                ('id', '267649766610622'),
                ('event_link', 'http://facebook.com/events/267649766610622'),
                ('name', 'Bachelor Party'),
                ('start_time',
                 datetime(2012, 2, 12, 3, 30),
                 ),
                ('end_time',
                 datetime(2012, 2, 12, 20)
                 ),
                 ('place_id', 'daf9c36fb8ab61e0c6bac9f045ebc27f'),
                 ])
        res_event_2 = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', '3f1743763812cec5508eef532ef7fedc'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 2),
                            ('places', 2),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event_1,
                                                res_event_2,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('daf9c36fb8ab61e0c6bac9f045ebc27f',
                                         place_1,
                                         ),
                                        ('3f1743763812cec5508eef532ef7fedc',
                                         place_2,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_same_name_different_place(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place_1 = OrderedDict([
                ('address', '6555 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101111'),
                ('longitude', '-118.33111'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        place_2 = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event_1 = OrderedDict([
                ('_id', '267649766610622'),
                ('facebook', OrderedDict([
                            ('name', 'Bachelor Party'),
                            ('id', '267649766610622'),
                            ('start_time',
                             datetime(2012, 2, 12, 3, 30),
                             ),
                            ('end_time',
                             datetime(2012, 2, 12, 20),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'ffd3b102-14c1-49dc-9bf2-6cb040a10bba'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.33111, 34.101111]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_1),
                            ])
                 ),
                ])
        event_2 = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_2),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event_1, event_2],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event_1 = OrderedDict([
                ('id', '267649766610622'),
                ('event_link', 'http://facebook.com/events/267649766610622'),
                ('name', 'Bachelor Party'),
                ('start_time',
                 datetime(2012, 2, 12, 3, 30),
                 ),
                ('end_time',
                 datetime(2012, 2, 12, 20)
                 ),
                 ('place_id', '7c7e5b25f012c8ccae9263a16aa4a027'),
                 ])
        res_event_2 = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 2),
                            ('places', 2),
                            ('coordinates', 2),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101111,-118.33111', [
                                                res_event_1,
                                                ]
                                         ),
                                        ('34.101593,-118.331231', [
                                                res_event_2,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('7c7e5b25f012c8ccae9263a16aa4a027',
                                         place_1,
                                         ),
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place_2,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_different_places(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place_1 = OrderedDict([
                ('address', '6555 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101111'),
                ('longitude', '-118.33111'),
                ('locality', 'Los Angeles'),
                ('name', 'Tabaco House'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        place_2 = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'HousePlay'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event_1 = OrderedDict([
                ('_id', '267649766610622'),
                ('facebook', OrderedDict([
                            ('name', 'Bachelor Party'),
                            ('id', '267649766610622'),
                            ('start_time',
                             datetime(2012, 2, 12, 3, 30),
                             ),
                            ('end_time',
                             datetime(2012, 2, 12, 20),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'ffd3b102-14c1-49dc-9bf2-6cb040a10bba'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.33111, 34.101111]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_1),
                            ])
                 ),
                ])
        event_2 = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place_2),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event_1, event_2],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        res_event_1 = OrderedDict([
                ('id', '267649766610622'),
                ('event_link', 'http://facebook.com/events/267649766610622'),
                ('name', 'Bachelor Party'),
                ('start_time',
                 datetime(2012, 2, 12, 3, 30),
                 ),
                ('end_time',
                 datetime(2012, 2, 12, 20)
                 ),
                 ('place_id', 'daf9c36fb8ab61e0c6bac9f045ebc27f'),
                 ])
        res_event_2 = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', '3f1743763812cec5508eef532ef7fedc'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 2),
                            ('places', 2),
                            ('coordinates', 2),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101111,-118.33111', [
                                                res_event_1,
                                                ]
                                         ),
                                        ('34.101593,-118.331231', [
                                                res_event_2,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('daf9c36fb8ab61e0c6bac9f045ebc27f',
                                         place_1,
                                         ),
                                        ('3f1743763812cec5508eef532ef7fedc',
                                         place_2,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_expired(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event],
            now=datetime(2012, 1, 25, 18, 38, 18, 62766),
            )

        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 0),
                            ('places', 0),
                            ('coordinates', 0),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ]),
                             ),
                            ]),
                 ),
                ])
        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_does_not_lose_precision(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location',
                                         [-118.33123112345,
                                           34.10159312345
                                           ],
                                         ),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.10159312345,-118.33123112345', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_bad_name(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', u'Jyv\xe4skyl\xe4n Paviljongin Huoltoparkki'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])

        res = api._get_results_by_coord(
            events=[event],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_empty(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        res = api._get_results_by_coord(
            events=[],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            )

        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 0),
                            ('places', 0),
                            ('coordinates', 0),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ]),
                             ),
                            ]),
                 ),
                ])
        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_get_results_by_coord_details_empty(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        res = api._get_results_by_coord(
            events=[],
            now=datetime(2012, 1, 23, 18, 38, 18, 62766),
            details=True,
            )

        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 0),
                            ('places', 0),
                            ('coordinates', 0),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ]),
                             ),
                            ]),
                 ),
                ])
        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_all_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        now = datetime(2012, 1, 23, 5, 26, 56)
        update = keys_coll.expects('update')
        change = OrderedDict([
                ('$set', OrderedDict([
                            ('last_used', now),
                            ]),
                 ),
                ('$inc', OrderedDict([
                            ('times_used', 1),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([('_id', 'foo host')]),
            change,
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('match', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.end_time', OrderedDict([
                                            ('$gt',
                                             datetime(2012, 1, 23),
                                             ),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('facebook.start_time', 1)],
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])
        find.returns([event])

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        query.has_attr(until='')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('now')
        utcnow.with_arg_count(0)
        utcnow.returns(now)

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )
        res = api._all(
            _request=fake_request,
            _datetime=fake_datetime,
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_all_until(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        now = datetime(2012, 1, 23, 5, 26, 56)
        update = keys_coll.expects('update')
        change = OrderedDict([
                ('$set', OrderedDict([
                            ('last_used', now),
                            ]),
                 ),
                ('$inc', OrderedDict([
                            ('times_used', 1),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([('_id', 'foo host')]),
            change,
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        until = datetime(2012, 2, 10, 21, 55, 43, 965992)
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('match', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.end_time', OrderedDict([
                                            ('$gt',
                                             datetime(2012, 1, 23),
                                             ),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.start_time', OrderedDict([
                                            ('$lte', until),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('facebook.start_time', 1)],
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])
        find.returns([event])

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        query.has_attr(until='2012-02-11T05:55:43.965992+00:00')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('now')
        utcnow.with_arg_count(0)
        utcnow.returns(now)

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )
        res = api._all(
            _request=fake_request,
            _datetime=fake_datetime,
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_all_until_error(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        query.has_attr(until='foo')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('now')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2012, 1, 23, 5, 26, 56))

        events_coll = fudge.Fake('events_coll')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )

        error = assert_raises(
            APIHTTPError,
            api._all,
            _request=fake_request,
            _datetime=fake_datetime,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid until parameter value"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_single_coord_simple(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        now = datetime(2012, 1, 23, 5, 26, 56)
        update = keys_coll.expects('update')
        change = OrderedDict([
                ('$set', OrderedDict([
                            ('last_used', now),
                            ]),
                 ),
                ('$inc', OrderedDict([
                            ('times_used', 1),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([('_id', 'foo host')]),
            change,
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query = OrderedDict([
                ('match.ubernear.location', [-118.331231,34.101593])
                ])
        find.with_args(
            OrderedDict([
                    ('$and', [query])
                    ]),
            sort=[('facebook.start_time', 1)],
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])
        find.returns([event])

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        query.has_attr(until='')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('now')
        utcnow.with_arg_count(0)
        utcnow.returns(now)

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )
        res = api._single_coord(
            '34.101593',
            '-118.331231',
            _request=fake_request,
            _datetime=fake_datetime,
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_single_coord_until(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        now = datetime(2012, 1, 23, 5, 26, 56)
        update = keys_coll.expects('update')
        change = OrderedDict([
                ('$set', OrderedDict([
                            ('last_used', now),
                            ]),
                 ),
                ('$inc', OrderedDict([
                            ('times_used', 1),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([('_id', 'foo host')]),
            change,
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        until = datetime(2012, 2, 10, 21, 55, 43, 965992)
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('match.ubernear.location', [-118.331231,34.101593])
                                ]),
                        OrderedDict([
                                ('facebook.start_time', OrderedDict([
                                            ('$lte', until),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('facebook.start_time', 1)],
            )

        place = OrderedDict([
                ('address', '6506 Hollywood Blvd'),
                ('country', 'US'),
                ('latitude', '34.101593'),
                ('longitude', '-118.331231'),
                ('locality', 'Los Angeles'),
                ('name', 'Playhouse'),
                ('postcode', '90028'),
                ('region', 'CA'),
                ])
        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('name', 'Birthday Party'),
                            ('id', '347324708616762'),
                            ('start_time',
                             datetime(2012, 1, 23, 20),
                             ),
                            ('end_time',
                             datetime(2012, 1, 24, 1, 30),
                             ),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('match', OrderedDict([
                            ('ubernear', OrderedDict([
                                        ('score', 100),
                                        ('place_id',
                                         'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                         ),
                                        ('source', 'factual'),
                                        ('location', [-118.331231, 34.101593]),
                                        ('matched', ['page']),
                                        ]),
                             ),
                            ('place', place),
                            ])
                 ),
                ])
        find.returns([event])

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        query.has_attr(until='2012-02-11T05:55:43.965992+00:00')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('now')
        utcnow.with_arg_count(0)
        utcnow.returns(now)

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )
        res = api._single_coord(
            '34.101593',
            '-118.331231',
            _request=fake_request,
            _datetime=fake_datetime,
            )

        res_event = OrderedDict([
                ('id', '347324708616762'),
                ('event_link', 'http://facebook.com/events/347324708616762'),
                ('name', 'Birthday Party'),
                ('start_time',
                 datetime(2012, 1, 23, 20),
                 ),
                ('end_time',
                 datetime(2012, 1, 24, 1, 30),
                 ),
                ('place_id', 'b5396d0eaff5f58e8405f7af4129cc7b'),
                ])
        expected = OrderedDict([
                ('status', OrderedDict([
                            ('code', 200),
                            ('message', 'OK'),
                            ]),
                 ),
                ('count', OrderedDict([
                            ('events', 1),
                            ('places', 1),
                            ('coordinates', 1),
                            ]),
                 ),
                ('data', OrderedDict([
                            ('events', OrderedDict([
                                        ('34.101593,-118.331231', [
                                                res_event,
                                                ]
                                         ),
                                        ]),
                             ),
                            ('places', OrderedDict([
                                        ('b5396d0eaff5f58e8405f7af4129cc7b',
                                         place,
                                         ),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        expected = json.dumps(
            expected,
            default=datetime.isoformat,
            )
        eq(res, expected)

    @fudge.with_fakes
    def test_single_coord_bad_coords(self):
        keys_coll = fudge.Fake('keys_coll')
        events_coll = fudge.Fake('events_coll')
        fake_request = fudge.Fake('request')
        fake_datetime = fudge.Fake('datetime')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )
        error = assert_raises(
            APIHTTPError,
            api._single_coord,
            'foo',
            '-118.331231',
            _request=fake_request,
            _datetime=fake_datetime,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid coordinates"}}'
            )
        eq(error.status, 400)

    @fudge.with_fakes
    def test_single_coord_until_error(self):
        keys_coll = fudge.Fake('keys_coll')
        keys_coll.remember_order()

        find_one = keys_coll.expects('find_one')
        find_one.with_args('foo host')
        key = OrderedDict([
                ('secret', 'foo secret'),
                ('disabled', False),
                ])
        find_one.returns(key)

        fake_request = fudge.Fake('request')
        query = fudge.Fake('query')
        _hash = ('ff1ccc056ab035a8808bba5f76a56f4425fd4588c14aaad625'
                 'c7bfb39ab92b92'
                 )
        query.has_attr(key=_hash)
        query.has_attr(until='foo')
        fake_request.has_attr(query=query)

        environ = fudge.Fake('environ')
        environ.remember_order()
        get = environ.expects('get')
        get.with_args('REMOTE_ADDR')
        get.returns('foo host')

        fake_request.has_attr(environ=environ)

        events_coll = fudge.Fake('events_coll')
        fake_datetime = fudge.Fake('datetime')

        api = EventAPI01(
            keys_coll=keys_coll,
            events_coll=events_coll,
            )
        error = assert_raises(
            APIHTTPError,
            api._single_coord,
            '34.101593',
            '-118.331231',
            _request=fake_request,
            _datetime=fake_datetime,
            )

        eq(
            error.output,
            '{"status": {"code": 400, "message": '
            '"Invalid until parameter value"}}'
            )
        eq(error.status, 400)
