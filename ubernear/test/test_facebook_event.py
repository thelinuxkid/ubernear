import fudge

from nose.tools import eq_ as eq
from collections import OrderedDict
from datetime import datetime
from facepy.exceptions import FacepyError

from ubernear import facebook_event

class FakeCursor(object):
    def __init__(self, events):
        self._events = events

    def __iter__(self):
        return self

    def next(self):
        try:
            return self._events.pop(0)
        except IndexError:
            raise StopIteration

    def count(self):
        return len(self._events)

class TestFacebookEvent(object):
    def setUp(self):
        fudge.clear_expectations()

    def _create_save_events_simple_fakes(
        self,
        events_coll=None,
        fake_log=None,
        ):
        self._fake_graph = fudge.Fake('graph')
        self._fake_graph.remember_order()

        batch = self._fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '267558763278075?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                OrderedDict([
                        ('title', 'event title 226680217397995'),
                        ('id', '226680217397995'),
                        ('start_time', '2012-02-26T08:00:00+00:00'),
                        ('end_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                OrderedDict([
                        ('title', 'event title 267558763278075'),
                        ('id', '267558763278075'),
                        ('start_time', '2012-02-26T08:00:00+00:00'),
                        ('end_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                ])

        if fake_log is None:
            self._fake_log = fudge.Fake('log')
            self._fake_log.remember_order()
        else:
            self._fake_log = fake_log

        debug = self._fake_log.expects('debug')
        debug.with_args(
            'Storing event 226680217397995',
            )

        debug = self._fake_log.next_call('debug')
        debug.with_args(
            'Storing event 267558763278075',
            )

        self._events_coll = events_coll
        if self._events_coll is None:
            self._events_coll = fudge.Fake('events_coll')
            self._events_coll.remember_order()

        update = self._events_coll.expects('update')
        save = OrderedDict([
                ('facebook.title', 'event title 226680217397995'),
                ('facebook.id', '226680217397995'),
                ('facebook.start_time',
                 datetime(2012, 2, 26, 8, 0, 0)
                 ),
                ('facebook.end_time',
                 datetime(2012, 2, 26, 11, 0, 0)
                 ),
                ('ubernear.source', 'facebook'),
                ('ubernear.lookup_completed',
                 datetime(2011, 11, 16, 2, 50, 32),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        update = self._events_coll.next_call('update')
        save = OrderedDict([
                ('facebook.title', 'event title 267558763278075'),
                ('facebook.id', '267558763278075'),
                ('facebook.start_time',
                 datetime(2012, 2, 26, 8, 0, 0)
                 ),
                ('facebook.end_time',
                 datetime(2012, 2, 26, 11, 0, 0)
                 ),
                ('ubernear.source', 'facebook'),
                ('ubernear.lookup_completed',
                 datetime(2011, 11, 16, 2, 50, 32),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

    @fudge.with_fakes
    def test_expire_simple(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        expired_coll = fudge.Fake('expired_coll')
        expired_coll.remember_order()

        find = events_coll.expects('find')
        last_week = datetime(2012, 3, 10, 7, 23, 19)
        end_parts = [
            OrderedDict([
                    ('facebook.end_time', OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('facebook.end_time', OrderedDict([
                                ('$lt', last_week),
                                ]),
                     ),
                    ]),
                    ]
        end_query = OrderedDict([
                ('$and', end_parts),
                ])
        false_query = OrderedDict([
                ('ubernear.lookup_failed.reason', 'False response'),
                ])
        unsupported_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'GraphMethodException error on get.*'
                             ': Unsupported get request..',
                             ),
                            ('$options', 'i'),
                            ]),
                 )
                ])
        alias_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*Some '
                             'of the aliases you requested do not exist.*'
                             ),
                            ('$options', 'i'),
                            ]),
                 )
                ])
        or_query = OrderedDict([
                ('$or',
                 [false_query,
                  unsupported_query,
                  alias_query,
                  ],
                 ),
                ])
        facebook_query = OrderedDict([
                ('ubernear.lookup_completed', OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        failed_query = OrderedDict([
                ('$and', [facebook_query, or_query]),
                ])
        find.with_args(
            OrderedDict([
                    ('$or',
                     [end_query,
                      failed_query,
                      ]
                     ),
                    ]),
            sort=[('facebook.end_time', 1)],
            )
        ubernear_1 = OrderedDict()
        ubernear_2 = OrderedDict()
        facebook_1 = OrderedDict([
                ('title', 'event title 226680217397995'),
                ('id', '226680217397995'),
                ('start_time', datetime(2012, 2, 26, 8, 0, 0)),
                ('end_time', datetime(2012, 2, 26, 11, 0, 0)),
                ('updated_time', datetime(2011, 10, 27, 22, 56, 42)),
                ])
        facebook_2 = OrderedDict([
                ('title', 'event title 267558763278075'),
                ('id', '267558763278075'),
                ('start_time', datetime(2011, 10, 28, 22, 0, 0)),
                ('end_time', datetime(2011, 10, 29, 4, 0, 0)),
                ('updated_time', datetime(2011, 10, 12, 19, 55, 58)),
                ])
        find.returns([
                OrderedDict([
                        ('_id', '226680217397995'),
                        ('facebook', facebook_1),
                        ('ubernear', ubernear_1),
                        ]),
                OrderedDict([
                        ('_id', '267558763278075'),
                        ('facebook', facebook_2),
                        ('ubernear', ubernear_2),
                        ]),
                ])

        update = expired_coll.expects('update')
        save = OrderedDict([
                ('facebook.title', 'event title 226680217397995'),
                ('facebook.id', '226680217397995'),
                ('facebook.start_time', datetime(2012, 2, 26, 8, 0, 0)),
                ('facebook.end_time', datetime(2012, 2, 26, 11, 0, 0)),
                ('facebook.updated_time',
                 datetime(2011, 10, 27, 22, 56, 42)
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        update = expired_coll.next_call('update')
        save = OrderedDict([
                ('facebook.title', 'event title 267558763278075'),
                ('facebook.id', '267558763278075'),
                ('facebook.start_time', datetime(2011, 10, 28, 22, 0, 0)),
                ('facebook.end_time', datetime(2011, 10, 29, 4, 0, 0)),
                ('facebook.updated_time',
                 datetime(2011, 10, 12, 19, 55, 58)
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        remove = events_coll.expects('remove')
        remove.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ])
            )

        remove = events_coll.next_call('remove')
        remove.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ])
            )

        fake_datetime = fudge.Fake('datetime')
        fake_datetime.remember_order()

        utcnow = fake_datetime.expects('utcnow')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2012, 3, 17, 7, 23, 19))

        facebook_event.expire(
            events_coll=events_coll,
            expired_coll=expired_coll,
            _datetime=fake_datetime,
            )

    @fudge.with_fakes
    def test_expire_place_ids(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        expired_coll = fudge.Fake('expired_coll')
        expired_coll.remember_order()

        find = events_coll.expects('find')
        last_week = datetime(2012, 3, 10, 7, 23, 19)
        end_parts = [
            OrderedDict([
                    ('facebook.end_time', OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('facebook.end_time', OrderedDict([
                                ('$lt', last_week),
                                ]),
                     ),
                    ]),
                    ]
        end_query = OrderedDict([
                ('$and', end_parts),
                ])
        false_query = OrderedDict([
                ('ubernear.lookup_failed.reason', 'False response'),
                ])
        unsupported_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'GraphMethodException error on get.*'
                             ': Unsupported get request..',
                             ),
                            ('$options', 'i'),
                            ]),
                 )
                ])
        alias_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*Some '
                             'of the aliases you requested do not exist.*'
                             ),
                            ('$options', 'i'),
                            ]),
                 )
                ])
        or_query = OrderedDict([
                ('$or',
                 [false_query,
                  unsupported_query,
                  alias_query,
                  ],
                 ),
                ])
        facebook_query = OrderedDict([
                ('ubernear.lookup_completed', OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        failed_query = OrderedDict([
                ('$and', [facebook_query, or_query]),
                ])
        find.with_args(
            OrderedDict([
                    ('$or',
                     [end_query,
                      failed_query,
                      ]
                     ),
                    ]),
            sort=[('facebook.end_time', 1)],
            )
        ubernear_1 = OrderedDict([
                ('place_ids',
                 ['bd2541a8-6a99-4da1-b82c-547bbebf6ee7'],
                 ),
                 ])
        ubernear_2 = OrderedDict([
                ('place_ids',
                 ['9807155b-1cba-4a34-8d0e-40e70dffcf8d'],
                 ),
                ])
        facebook_1 = OrderedDict([
                ('title', 'event title 226680217397995'),
                ('id', '226680217397995'),
                ('start_time', datetime(2012, 2, 26, 8, 0, 0)),
                ('end_time', datetime(2012, 2, 26, 11, 0, 0)),
                ('updated_time', datetime(2011, 10, 27, 22, 56, 42)),
                ])
        facebook_2 = OrderedDict([
                ('title', 'event title 267558763278075'),
                ('id', '267558763278075'),
                ('start_time', datetime(2011, 10, 28, 22, 0, 0)),
                ('end_time', datetime(2011, 10, 29, 4, 0, 0)),
                ('updated_time', datetime(2011, 10, 12, 19, 55, 58)),
                ])
        find.returns([
                OrderedDict([
                        ('_id', '226680217397995'),
                        ('facebook', facebook_1),
                        ('ubernear', ubernear_1),
                        ]),
                OrderedDict([
                        ('_id', '267558763278075'),
                        ('facebook', facebook_2),
                        ('ubernear', ubernear_2),
                        ]),
                ])

        update = expired_coll.expects('update')
        save = OrderedDict([
                ('facebook.title', 'event title 226680217397995'),
                ('facebook.id', '226680217397995'),
                ('facebook.start_time', datetime(2012, 2, 26, 8, 0, 0)),
                ('facebook.end_time', datetime(2012, 2, 26, 11, 0, 0)),
                ('facebook.updated_time',
                 datetime(2011, 10, 27, 22, 56, 42)
                 ),
                ])
        add_each = OrderedDict([
                ('ubernear.place_ids', OrderedDict([
                            ('$each',
                             ['bd2541a8-6a99-4da1-b82c-547bbebf6ee7'],
                             ),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ('$addToSet', add_each),
                    ]),
            upsert=True,
            safe=True,
            )

        update = expired_coll.next_call('update')
        save = OrderedDict([
                ('facebook.title', 'event title 267558763278075'),
                ('facebook.id', '267558763278075'),
                ('facebook.start_time', datetime(2011, 10, 28, 22, 0, 0)),
                ('facebook.end_time', datetime(2011, 10, 29, 4, 0, 0)),
                ('facebook.updated_time',
                 datetime(2011, 10, 12, 19, 55, 58)
                 ),
                ])
        add_each = OrderedDict([
                ('ubernear.place_ids', OrderedDict([
                            ('$each',
                             ['9807155b-1cba-4a34-8d0e-40e70dffcf8d'],
                             ),
                            ]),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ('$addToSet', add_each),
                    ]),
            upsert=True,
            safe=True,
            )

        remove = events_coll.expects('remove')
        remove.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ])
            )

        remove = events_coll.next_call('remove')
        remove.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ])
            )

        fake_datetime = fudge.Fake('datetime')
        fake_datetime.remember_order()

        utcnow = fake_datetime.expects('utcnow')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2012, 3, 17, 7, 23, 19))

        facebook_event.expire(
            events_coll=events_coll,
            expired_coll=expired_coll,
            _datetime=fake_datetime,
            )

    @fudge.with_fakes
    def test_save_events_simple(self):
        self._create_save_events_simple_fakes()

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ('ubernear', OrderedDict([
                                ('place_ids',
                                 ['cb036268-2ba8-47db-906c-ca3b66d4da73'],
                                 ),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('_id', '267558763278075'),
                    ('ubernear', OrderedDict([
                                ('place_ids',
                                 ['11ee45ad-2ad0-4bbb-9d9d-a107b88cc579'],
                                 ),
                                ]),
                     ),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=self._events_coll,
            graph=self._fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=self._fake_log,
            )

    @fudge.with_fakes
    def test_save_events_empty_events(self):
        fake_log = fudge.Fake('log')
        fake_graph = fudge.Fake('graph')
        events_coll = fudge.Fake('events_coll')

        facebook_event._save_events(
            events=[],
            events_coll=events_coll,
            graph=fake_graph,
            now='foo now',
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_error(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '267558763278075?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                FacepyError('foo error'),
                ])

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                 datetime(2011, 11, 16, 2, 50, 32)
                 ),
                ('ubernear.lookup_failed.reason', 'foo error'),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')

        events = [
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_false(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '267558763278075?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([False])

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                 datetime(2011, 11, 16, 2, 50, 32)
                 ),
                ('ubernear.lookup_failed.reason', 'False response'),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')

        events = [
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_null(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([None])

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                 datetime(2011, 11, 16, 2, 50, 32)
                 ),
                ('ubernear.lookup_failed.reason', 'Null response'),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )
    @fudge.with_fakes
    def test_save_events_different_id(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                OrderedDict([
                        ('title', 'event title 226680217397995'),
                        ('id', 'foo id'),
                        ('start_time', '2012-02-26T08:00:00+00:00'),
                        ('end_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('error')
        debug.with_args(
            'Facebook returned information for an event other than '
            '226680217397995. Skipping event.'
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                 datetime(2011, 11, 16, 2, 50, 32)
                 ),
                ('ubernear.lookup_failed.reason',
                 'Response id is different',
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_location(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '267558763278075?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                OrderedDict([
                        ('title', 'event title 226680217397995'),
                        ('id', '226680217397995'),
                        ('start_time', '2012-02-26T08:00:00+00:00'),
                        ('end_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                OrderedDict([
                        ('title', 'event title 267558763278075'),
                        ('id', '267558763278075'),
                        ('venue', OrderedDict([
                                    ('latitude', 37.78739108147),
                                    ('longitude', -122.40981954615),
                                    ])
                         ),
                        ('start_time', '2012-02-26T08:00:00+00:00'),
                        ('end_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Storing event 226680217397995',
            )

        debug = fake_log.next_call('debug')
        debug.with_args(
            'Storing event 267558763278075',
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('facebook.title', 'event title 226680217397995'),
                ('facebook.id', '226680217397995'),
                ('facebook.start_time',
                 datetime(2012, 2, 26, 8, 0, 0)
                 ),
                ('facebook.end_time',
                 datetime(2012, 2, 26, 11, 0, 0)
                 ),
                ('ubernear.source', 'facebook'),
                ('ubernear.lookup_completed',
                 datetime(2011, 11, 16, 2, 50, 32),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        update = events_coll.next_call('update')
        save = OrderedDict([
                ('facebook.title', 'event title 267558763278075'),
                ('facebook.id', '267558763278075'),
                ('facebook.venue.latitude', 37.78739108147),
                ('facebook.venue.longitude', -122.40981954615),
                ('facebook.start_time',
                 datetime(2012, 2, 26, 8, 0, 0)
                 ),
                ('facebook.end_time',
                 datetime(2012, 2, 26, 11, 0, 0)
                 ),
                ('ubernear.source', 'facebook'),
                ('ubernear.lookup_completed',
                 datetime(2011, 11, 16, 2, 50, 32),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_some_work(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '267558763278075?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                OrderedDict([
                        ('title', 'event title 226680217397995'),
                        ('id', 'foo id'),
                        ('start_time', '2012-01-15T21:23+00:00'),
                        ('end_time', '2012-01-16T05:22+00:00'),
                        ]),
                OrderedDict([
                        ('title', 'event title 267558763278075'),
                        ('id', '267558763278075'),
                        ('start_time', '2012-01-15T21:23+00:00'),
                        ('end_time', '2012-01-16T05:22+00:00'),
                        ]),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('error')
        debug.with_args(
            'Facebook returned information for an event other than '
            '226680217397995. Skipping event.'
            )

        debug = fake_log.expects('debug')
        debug.with_args(
            'Storing event 267558763278075',
            )

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                 datetime(2011, 11, 16, 2, 50, 32)
                 ),
                ('ubernear.lookup_failed.reason',
                 'Response id is different',
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        update = events_coll.next_call('update')
        save = OrderedDict([
                ('facebook.title', 'event title 267558763278075'),
                ('facebook.id', '267558763278075'),
                ('facebook.start_time',
                 datetime(2012, 1, 15, 21, 23),
                 ),
                ('facebook.end_time',
                 datetime(2012, 1, 16, 5, 22),
                 ),
                ('ubernear.source', 'facebook'),
                ('ubernear.lookup_completed',
                 datetime(2011, 11, 16, 2, 50, 32),
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_no_start_time(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                OrderedDict([
                        ('title', 'event title 226680217397995'),
                        ('id', '226680217397995'),
                        ('end_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                ])

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                datetime(2011, 11, 16, 2, 50, 32)
                ),
                ('ubernear.lookup_failed.reason',
                 'Missing start_time or end_time',
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_save_events_no_end_time(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        batch = fake_graph.expects('batch')
        request = [
            OrderedDict([
                    ('method', 'GET'),
                    ('relative_url', '226680217397995?date_format=c'),
                    ]),
            ]
        batch.with_args(request)
        batch.returns([
                OrderedDict([
                        ('title', 'event title 226680217397995'),
                        ('id', '226680217397995'),
                        ('start_time', '2012-02-26T11:00:00+00:00'),
                        ]),
                ])

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        update = events_coll.expects('update')
        save = OrderedDict([
                ('ubernear.lookup_failed.when',
                datetime(2011, 11, 16, 2, 50, 32)
                ),
                ('ubernear.lookup_failed.reason',
                 'Missing start_time or end_time',
                 ),
                ])
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', save),
                    ]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')

        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            ]
        facebook_event._save_events(
            events=events,
            events_coll=events_coll,
            graph=fake_graph,
            now=datetime(2011, 11, 16, 2, 50, 32),
            _log=fake_log,
            )

    @fudge.with_fakes
    def test_update_facebook_simple(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        null_query = OrderedDict([
                ('ubernear.lookup_failed.reason',
                 'Null response',
                 ),
                ])
        validating_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*: '
                             'Error validating application..'
                             )
                            ]),
                 )
                ])
        retry_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*: '
                             'An unexpected error has occurred. '
                             'Please retry your request later..'
                             )
                            ]),
                 )
                ])
        or_query = OrderedDict([
                ('$or',
                 [null_query,
                  validating_query,
                  retry_query,
                  ],
                 ),
                ])
        match_query = OrderedDict([
                ('ubernear.lookup_completed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        and_query = OrderedDict([
                ('$and', [match_query, or_query]),
                ])
        query = OrderedDict([
                ('$or',
                 [match_query, and_query],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', 1)],
            )

        ubernear_1 = OrderedDict([
                ('fetched', datetime(2011, 11, 14, 1, 15, 53)),
                ])
        ubernear_2 = OrderedDict([
                ('fetched', datetime(2011, 11, 15, 2, 36, 21)),
                ])
        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ('ubernear', ubernear_1),
                    ]),
            OrderedDict([
                    ('_id', '267558763278075'),
                    ('ubernear', ubernear_2),
                    ]),
            ]
        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        info = fake_log.expects('info')
        info.with_args(
            'Fetching 2 events',
            )

        self._create_save_events_simple_fakes(
            events_coll=events_coll,
            fake_log=fake_log,
            )

        fake_datetime = fudge.Fake('datetime')
        fake_datetime.remember_order()

        utcnow = fake_datetime.expects('utcnow')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2011, 11, 16, 2, 50, 32))

        found_work = facebook_event.update_facebook(
            events_coll=self._events_coll,
            graph=self._fake_graph,
            _log=self._fake_log,
            _datetime=fake_datetime,
            )

        eq(found_work, True)

    @fudge.with_fakes
    def test_update_facebook_more_than_50(self):
        fake_graph = fudge.Fake('graph')
        fake_graph.remember_order()

        def batch_fakes(fake_graph, start, end):
            if start == 1:
                batch = fake_graph.expects('batch')
            else:
                batch = fake_graph.next_call('batch')
            request = [
                OrderedDict([
                        ('method', 'GET'),
                        ('relative_url', '{i}?date_format=c'.format(
                            i=i,
                            ),
                         ),
                        ])
                for i in xrange(start,end)
                ]
            batch.with_args(request)

            batch_response = []
            for i in xrange(start, end):
                min_ = 15
                sec = i
                if sec > 59:
                    min_ += sec/59
                    sec %= 59
                response = OrderedDict([
                        ('title', 'event title {i}'.format(i=i)),
                        ('id', '{i}'.format(i=i)),
                        ('start_time',
                         '2012-02-26T08:{:02}:{:02}+00:00'.format(
                             min_,
                             sec
                             ),
                         ),
                        ('end_time',
                         '2012-02-26T11:{:02}:{:02}+00:00'.format(
                             min_,
                             sec,
                             ),
                         ),
                        ])
                batch_response.append(response)

            batch.returns(batch_response)

        batch_fakes(fake_graph, 1, 50+1)
        batch_fakes(fake_graph, 51, 100+1)
        batch_fakes(fake_graph, 101, 105+1)

        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        null_query = OrderedDict([
                ('ubernear.lookup_failed.reason',
                 'Null response',
                 ),
                ])
        validating_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*: '
                             'Error validating application..'
                             )
                            ]),
                 )
                ])
        retry_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*: '
                             'An unexpected error has occurred. '
                             'Please retry your request later..'
                             )
                            ]),
                 )
                ])
        or_query = OrderedDict([
                ('$or',
                 [null_query,
                  validating_query,
                  retry_query,
                  ],
                 ),
                ])
        match_query = OrderedDict([
                ('ubernear.lookup_completed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        and_query = OrderedDict([
                ('$and', [match_query, or_query]),
                ])
        query = OrderedDict([
                ('$or',
                 [match_query, and_query],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', 1)],
            )

        events = []
        for i in xrange(1,105+1):
            min_ = 15
            sec = i
            if sec > 59:
                min_ += sec/59
                sec %= 59
            ubernear = OrderedDict([
                    ('fetched', datetime(2011, 11, 14, 1, min_, sec)),
                    ])
            events.append(
                OrderedDict([
                        ('_id', '{i}'.format(i=i)),
                        ('ubernear', ubernear),
                        ])
                )
        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        info = fake_log.expects('info')
        info.with_args('Fetching 105 events')

        def debug_fakes(fake_log, i):
            if i == 1:
                debug = fake_log.expects('debug')
            else:
                debug = fake_log.next_call('debug')
            debug.with_args(
                'Storing event {i}'.format(i=i),
                )

        [debug_fakes(fake_log,i) for i in xrange(1,105+1)]

        def update_fakes(events_coll, i):
            if i == 1:
                update = events_coll.expects('update')
            else:
                update = events_coll.next_call('update')

            min_ = 15
            sec = i
            if sec > 59:
                min_ += sec/59
                sec %= 59

            save = OrderedDict([
                    ('facebook.title', 'event title {i}'.format(i=i)),
                    ('facebook.id', '{i}'.format(i=i)),
                    ('facebook.start_time',
                     datetime(2012,2,26,8,min_,sec)
                     ),
                    ('facebook.end_time',
                     datetime(2012,2,26,11,min_,sec)
                     ),
                    ('ubernear.source', 'facebook'),
                    ('ubernear.lookup_completed',
                     datetime(2011, 10, 16, 2, 50, 32),
                     ),
                    ])
            update.with_args(
                OrderedDict([
                        ('_id', '{i}'.format(i=i)),
                        ]),
                OrderedDict([
                        ('$set', save),
                        ]),
                upsert=True,
                safe=True,
                )

        [update_fakes(events_coll,i) for i in xrange(1,105+1)]

        fake_datetime = fudge.Fake('datetime')
        fake_datetime.remember_order()

        utcnow = fake_datetime.expects('utcnow')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2011, 10, 16, 2, 50, 32))

        found_work = facebook_event.update_facebook(
            events_coll=events_coll,
            graph=fake_graph,
            _log=fake_log,
            _datetime=fake_datetime,
            )

        eq(found_work, True)

    @fudge.with_fakes
    def test_update_facebook_no_work(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        null_query = OrderedDict([
                ('ubernear.lookup_failed.reason',
                 'Null response',
                 ),
                ])
        validating_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*: '
                             'Error validating application..'
                             )
                            ]),
                 )
                ])
        retry_query = OrderedDict([
                ('ubernear.lookup_failed.reason', OrderedDict([
                            ('$regex',
                             'OAuthException error on get.*: '
                             'An unexpected error has occurred. '
                             'Please retry your request later..'
                             )
                            ]),
                 )
                ])
        or_query = OrderedDict([
                ('$or',
                 [null_query,
                  validating_query,
                  retry_query,
                  ],
                 ),
                ])
        match_query = OrderedDict([
                ('ubernear.lookup_completed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        and_query = OrderedDict([
                ('$and', [match_query, or_query]),
                ])
        query = OrderedDict([
                ('$or',
                 [match_query, and_query],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', 1)],
            )

        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        fake_datetime = fudge.Fake('datetime')
        fake_datetime.remember_order()

        utcnow = fake_datetime.expects('utcnow')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2011, 10, 16, 2, 50, 32))

        fake_graph = fudge.Fake('graph')

        fake_log = fudge.Fake('log')

        found_work = facebook_event.update_facebook(
            events_coll=events_coll,
            graph=fake_graph,
            _log=fake_log,
            _datetime=fake_datetime,
            )

        eq(found_work, False)


    @fudge.with_fakes
    def test_update_facebook_process_all(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        find.with_arg_count(0)
        ubernear_1 = OrderedDict([
                ('fetched', datetime(2011, 11, 14, 1, 15, 53)),
                ])
        ubernear_2 = OrderedDict([
                ('fetched', datetime(2011, 11, 15, 2, 36, 21)),
                ])
        events = [
            OrderedDict([
                    ('_id', '226680217397995'),
                    ('ubernear', ubernear_1),
                    ]),
            OrderedDict([
                    ('_id', '267558763278075'),
                    ('ubernear', ubernear_2),
                    ]),
            ]
        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        info = fake_log.expects('info')
        info.with_args(
            'Fetching 2 events',
            )

        self._create_save_events_simple_fakes(
            events_coll=events_coll,
            fake_log=fake_log,
            )

        fake_datetime = fudge.Fake('datetime')
        fake_datetime.remember_order()

        utcnow = fake_datetime.expects('utcnow')
        utcnow.with_arg_count(0)
        utcnow.returns(datetime(2011, 11, 16, 2, 50, 32))

        found_work = facebook_event.update_facebook(
            events_coll=self._events_coll,
            graph=self._fake_graph,
            process_all=True,
            _log=self._fake_log,
            _datetime=fake_datetime,
              )

        eq(found_work, True)

# TODO Add update_venue tests
# TODO Add update_coordinate tests
