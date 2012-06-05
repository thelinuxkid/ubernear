import bson
import pymongo
import fudge

from nose.tools import eq_ as eq
from collections import OrderedDict
from datetime import datetime

from ubernear import event_location

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

    def count(self, *args):
        return len(self._events)

class TestEventLocation(object):
    def setUp(self):
        fudge.clear_expectations()

    @fudge.with_fakes
    def test_match_with_place_event_normalized(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '6506 HOLLYWOOD BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        find.returns([])

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ('city', 'Los Angeles'),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 200),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              ]
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_event_and_place_normalized(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'LA'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])
        find_one.returns(place)

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '6506 HOLLYWOOD BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        find.returns([])

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ('city', 'Los Angeles'),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 200),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              ]
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_normalized_search(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '6506 HOLLYWOOD BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'LA'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])
        find.returns([place])

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ('city', 'Los Angeles'),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 200),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'normalized'),
                            ('matched',
                             ['address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              ]
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_seen(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '6506 HOLLYWOOD BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        find.returns([place])

        database = fudge.Fake('database')
        database.remember_order()

        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )

        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 300),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'coordinate'),
                            ('matched',
                             ['coord',
                              'address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              ]
                             ),
                            ('distance', 47.72857352727635),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_place_has_no_coord(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                        ('source', 'factual'),
                        ])
                 ),
                ])
        find_one.returns(place)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        error = fake_log.expects('error')
        error.with_args(
            'Place cb036268-2ba8-47db-906c-ca3b66d4da73 '
            'has no indexed coordinates. Skipping place'
            )

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('id', '347324708616762'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_coord_db_error(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        command.raises(
            pymongo.errors.OperationFailure('foo error')
            )

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        error = fake_log.expects('error')
        error.with_args(
            'GeoNear search returned error "foo error" for event '
            '347324708616762'
            )

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('id', '347324708616762'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_coord_res_not_ok(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        res = OrderedDict([
                ('ok', 2.0),
                ])
        command.returns(res)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        error = fake_log.expects('error')
        error.with_args(
            'GeoNear search failed for event 347324708616762'
            )

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('id', '347324708616762'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_coord_distance_greater_than_100(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 1947.72857352727635),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        error = fake_log.expects('error')
        error.with_args(
            'GeoNear search returned distance 1947.72857353 which is '
            'greater than 100 meters for event 347324708616762 and '
            'place cb036268-2ba8-47db-906c-ca3b66d4da73. Skipping place.'
            )

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('id', '347324708616762'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_coord_no_result(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        res = OrderedDict([
                ('ok', 1.0),
                ('results', []),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('id', '347324708616762'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_coord_and_name_match(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ]),
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse'),
                            ('venue', OrderedDict([
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'coordinate'),
                            ('matched', ['coord', 'name']),
                            ('distance', 47.72857352727635),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_coord_and_address_match(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ]),
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'coordinate'),
                            ('matched', ['coord', 'address']),
                            ('distance', 47.72857352727635),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_page_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['page']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_address_and_name_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse Hollywood'),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['address', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_address_and_owner_name_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('owner', OrderedDict([
                                        ('name', 'Playhouse Hollywood'),
                                        ]),
                             ),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['address', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_address_name_and_owner_name_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse H'),
                            ('owner', OrderedDict([
                                        ('name', 'Playhouse Hollywood'),
                                        ]),
                             ),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['address', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse H'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_address_and_owner_name_match_name_doesnt(
        self,
        ):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Plyh'),
                            ('owner', OrderedDict([
                                        ('name', 'Playhouse Hollywood'),
                                        ]),
                             ),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['address', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_address_and_name_match_owner_name_doesnt(
        self,
        ):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse Hollywood'),
                            ('owner', OrderedDict([
                                        ('name', 'Plyh'),
                                        ]),
                             ),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['address', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_address_and_locality_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ('city', 'Los Angeles'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched', ['address', 'locality']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_postcode_and_address_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '6506 HOLLYWOOD BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find.returns([place])

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('id', '347324708616762'),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 200),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'normalized'),
                            ('matched',
                             ['address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              ]
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_name_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse Hollywood'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_owner_name_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('owner', OrderedDict([
                                        ('name', 'Playhouse Hollywood')
                                        ])
                             ),
                            ]),

                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_locality_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('city', 'Los Angeles'),
                                        ])
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_region_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('state', 'California'),
                                        ])
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_country_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('country', 'United States'),
                                        ])
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_all_match(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse Hollywood'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('street', '6506 Hollywood Blvd'),
                                        ('city', 'Los Angeles'),
                                        ('state', 'CA'),
                                        ('country', 'US'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 500),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'coordinate'),
                            ('matched',
                             ['page',
                              'coord',
                              'address',
                              'locality',
                              'region',
                              'country',
                              'name',
                              ],
                             ),
                            ('distance', 47.72857352727635),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_normalized_all_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '6506 HOLLYWOOD BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'LA'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('name', 'PLAYHOUSE'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])
        find.returns([place])

        database = fudge.Fake('database')

        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.331231, 34.101593]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('latitude', 34.101593),
                                        ('longitude', -118.331231),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '6506 HOLLYWOOD BLVD'),
                            ('city', 'LOS ANGELES'),
                            ('country', 'US'),
                            ('state', 'CA'),
                            ('zip4', '6210'),
                            ('zip5', '90028'),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 600),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'coordinate'),
                            ('matched',
                             ['page',
                              'coord',
                              'address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              'name',
                              ]
                             ),
                            ('distance', 47.72857352727635),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_no_match(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'f6ecfadd-3e8b-419a-8ea8-7d27d41d1314'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'f6ecfadd-3e8b-419a-8ea8-7d27d41d1314'),
                ('info', OrderedDict([
                        ('address', '6308 S Broadway'),
                        ('country', 'US'),
                        ('locality', 'Los Angeles'),
                        ('name', 'Lopez Interiors'),
                        ('postcode', '90003'),
                        ('region', 'CA'),
                        ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.278282, 33.982056]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '294647503893475'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('city', 'Dallas'),
                                        ('country', 'United States'),
                                        ('state', 'Texas'),
                                        ('street', '1508 Cadiz'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['f6ecfadd-3e8b-419a-8ea8-7d27d41d1314'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_source_required_fields_error(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        error = fake_log.expects('error')
        error.with_args(
            'Place cca1d8d3-83ef-44d8-876c-23592b14f6e4 is missing '
            'field "address". Skipping place'
            )

        database = fudge.Fake('database')

        match = event_location._match_with_place(
            event=event,
            place_ids=['cca1d8d3-83ef-44d8-876c-23592b14f6e4'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_place_source_some_missing_fields(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.39647460, 34.1671240]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )

        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 44.07166072513344),
                                ('obj', place)
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'The Eclectic Company Theatre'),
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('latitude', 34.1671240),
                                        ('longitude', -118.39647460),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'coordinate'),
                            ('matched', ['coord', 'name']),
                            ('distance', 44.07166072513344),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_higher(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                    ])
            )
        place = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '07d352d5-bcb6-48db-8420-6386ad4418aa'),
                    ])
            )
        place = OrderedDict([
                ('_id', '07d352d5-bcb6-48db-8420-6386ad4418aa'),
                ('info', OrderedDict([
                        ('address', '6506 Hollywood Blvd'),
                        ('category', 'Shopping > Gift & Novelty Stores'),
                        ('country', 'US'),
                        ('id', '07d352d5-bcb6-48db-8420-6386ad4418aa'),
                        ('latitude', '33.982056'),
                        ('longitude', '-118.278282'),
                        ('locality', 'Hollywood'),
                        ('name', 'Misha International'),
                        ('postcode', '90028'),
                        ('region', 'CA'),
                        ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331858, 34.101514]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse Hollywood'),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')
        database = fudge.Fake('database')

        place_ids = ['cb036268-2ba8-47db-906c-ca3b66d4da73',
                     '07d352d5-bcb6-48db-8420-6386ad4418aa',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'name'],
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_higher(self):
        place_1 = OrderedDict([
                ('_id', '07d352d5-bcb6-48db-8420-6386ad4418aa'),
                ('info', OrderedDict([
                        ('address', '6506 Hollywood Blvd'),
                        ('category', 'Shopping > Gift & Novelty Stores'),
                        ('country', 'US'),
                        ('id', '07d352d5-bcb6-48db-8420-6386ad4418aa'),
                        ('latitude', '33.982056'),
                        ('longitude', '-118.278282'),
                        ('locality', 'Hollywood'),
                        ('name', 'Misha International'),
                        ('postcode', '90028'),
                        ('region', 'CA'),
                        ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331858, 34.101514]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])
        place_2 = OrderedDict([
                ('_id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                ('info', OrderedDict([
                            ('id', 'cb036268-2ba8-47db-906c-ca3b66d4da73'),
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.331231, 34.101593]),
                            ('source', 'factual'),
                            ])
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.33145997011, 34.10120790975]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )

        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place_1),
                                ]),
                        OrderedDict([
                                ('dis', 47.72857352727635),
                                ('obj', place_2),
                                ]),
                        ],
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '347324708616762'),
                ('facebook', OrderedDict([
                            ('location', 'Playhouse Hollywood'),
                            ('venue', OrderedDict([
                                        ('street', '6506 Hollywood Blvd'),
                                        ('longitude', -118.33145997011),
                                        ('latitude', 34.10120790975),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 300),
                            ('place_id',
                             'cb036268-2ba8-47db-906c-ca3b66d4da73'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.331231, 34.101593]),
                            ('search_type', 'coordinate'),
                            ('matched',
                             ['coord', 'address', 'name'],
                             ),
                            ('distance', 47.72857352727635),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse Hollywood'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_has_page(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Valley Village'),
                                        ('id', '75054390179'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.241399, 34.049053]),
                            ('search_type', 'place_ids'),
                            ('matched', ['page']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ('latitude', 34.049053),
                            ('longitude', -118.241399),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_has_page(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Valley Village'),
                                        ('id', '75054390179'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     'cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.241399, 34.049053]),
                            ('search_type', 'place_ids'),
                            ('matched', ['page']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ('latitude', 34.049053),
                            ('longitude', -118.241399),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_has_address_and_postcode(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '5312 LAUREL CANYON BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        place_1 = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])
        place_2 = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find.returns([place_1, place_2])

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Los Angeles'),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '5312 LAUREL CANYON BLVD'),
                            ('country', 'US'),
                            ('city', 'LOS ANGELES'),
                            ('state', 'CA'),
                            ('zip5', '91607'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'normalized'),
                            ('matched',
                             ['address',
                              'region',
                              'postcode',
                              'country'
                              ],
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_has_address_and_postcode(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '5312 LAUREL CANYON BLVD'),
                    ('normalized.city', 'LOS ANGELES'),
                    ])
            )
        place_1 = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        place_2 = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])
        find.returns([place_1, place_2])

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Los Angeles'),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '5312 LAUREL CANYON BLVD'),
                            ('country', 'US'),
                            ('city', 'LOS ANGELES'),
                            ('state', 'CA'),
                            ('zip5', '91607'),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'normalized'),
                            ('matched',
                             ['address',
                              'region',
                              'postcode',
                              'country'
                              ],
                             ),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_has_address_and_locality(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '55555'),
                            ('region', 'GA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'Rafu California Patrol'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Valley Village'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'locality']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_has_address_and_locality(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '55555'),
                            ('region', 'GA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'Rafu California Patrol'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Valley Village'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     'cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'locality']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_has_coord_and_address(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.39647460, 34.1671240]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place_1 = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])
        place_2 = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])

        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 44.07166072513344),
                                ('obj', place_1),
                                ]),
                        OrderedDict([
                                ('dis', 44.07166072513344),
                                ('obj', place_2),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'Rafu California Patrol'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('longitude', -118.39647460),
                                        ('latitude', 34.1671240),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'coordinate'),
                            ('matched', ['coord', 'address']),
                            ('distance', 44.07166072513344),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_has_coord_and_address(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.39647460, 34.1671240]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place_1 = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        place_2 = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 44.07166072513344),
                                ('obj', place_1),
                                ]),
                        OrderedDict([
                                ('dis', 44.07166072513344),
                                ('obj', place_2),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'Rafu California Patrol'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('longitude', -118.39647460),
                                        ('latitude', 34.1671240),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=[],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'coordinate'),
                            ('matched', ['coord', 'address']),
                            ('distance', 44.07166072513344),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    # Coord match will always be first
    @fudge.with_fakes
    def test_match_with_place_first_has_coord_and_name(self):
        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.39647460, 34.1671240]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])
        res = OrderedDict([
                ('ok', 1.0),
                ('results', [
                        OrderedDict([
                                ('dis', 44.07166072513344),
                                ('obj', place),
                                ]),
                        ]
                 ),
                ])
        command.returns(res)

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )
        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])
        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'The Eclectic Company Theatre'),
                            ('venue', OrderedDict([
                                        ('street', '320 E 2nd St'),
                                        ('longitude', -118.39647460),
                                        ('latitude', 34.1671240),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'coordinate'),
                            ('matched', ['coord', 'name']),
                            ('distance', 44.07166072513344),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_has_address_name_and_region(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '55555'),
                            ('region', 'GA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'The Eclectic Company Theatre'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('state', 'CA'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'region', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_has_address_name_and_region(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '55555'),
                            ('region', 'GA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'The Eclectic Company Theatre'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('state', 'CA'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     'cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'region', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_first_has_address_name_and_country(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'CA'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '55555'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'The Eclectic Company Theatre'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('state', 'CA'),
                                        ('country', 'US'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'region', 'country', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_second_has_address_name_and_country(self):
        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                    ])
            )

        place = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'CA'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '55555'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])

        find_one.returns(place)

        find_one = places_coll.next_call('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                    ])
            )

        place = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])

        find_one.returns(place)

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('location', 'The Eclectic Company Theatre'),
                            ('venue', OrderedDict([
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('state', 'CA'),
                                        ('country', 'US'),
                                        ]),
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        fake_log = fudge.Fake('log')

        place_ids = ['31d0e5dd-9929-4573-9b6b-5fd0d62eadb9',
                     'cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                     ]
        match = event_location._match_with_place(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['address', 'region', 'country', 'name']),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_place_multiple(self):
        place_1 = OrderedDict([
                ('_id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                ('info', OrderedDict([
                            ('id', '31d0e5dd-9929-4573-9b6b-5fd0d62eadb9'),
                            ('address', '626 Wilshire Blvd'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Dacom America'),
                            ('postcode', '90017'),
                            ('region', 'CA'),
                            ]),
                 ),
                ('ubernear', OrderedDict([
                            ('location', [-118.241399, 34.049053]),
                            ('source', 'factual'),
                            ]),
                 ),
                ])
        place_2 = OrderedDict([
                ('_id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                ('info', OrderedDict([
                            ('id', 'cca1d8d3-83ef-44d8-876c-23592b14f6e4'),
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.396004, 34.167198]),
                            ('source', 'factual'),
                            ]),
                  ),
                ])
        place_3 = OrderedDict([
                ('_id', '10f20e75-8c9e-4ebc-879b-6104c42e0b6c'),
                ('info', OrderedDict([
                            ('id', '10f20e75-8c9e-4ebc-879b-6104c42e0b6c'),
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ]),
                 ),
                 ('ubernear', OrderedDict([
                            ('location', [-118.256867, 34.048416]),
                            ('source', 'factual'),
                            ])
                  ),
                ('facebook', OrderedDict([
                            ('pages', [
                                    OrderedDict([
                                            ('id', '75054390179'),
                                            ]),
                                    ],
                             ),
                            ]),
                 ),
                ])

        database = fudge.Fake('database')
        database.remember_order()

        places_coll = fudge.Fake('places_coll')
        places_coll.remember_order()
        places_coll.has_attr(name='places')

        command = database.expects('command')
        command.with_args(
            bson.SON(
                OrderedDict([
                        ('geoNear', 'places'),
                        ('near', [-118.39647460, 34.1671240]),
                        ])
                ),
            spherical=True,
            maxDistance=1.5696e-05,
            distanceMultiplier=6371000,
            )
        res = OrderedDict([
                ('ok', 1.0),
                ('results', []),
                ])
        command.returns(res)

        find_one = places_coll.expects('find_one')
        find_one.with_args(
            OrderedDict([
                    ('_id', '10f20e75-8c9e-4ebc-879b-6104c42e0b6c'),
                    ])
            )
        find_one.returns(place_3)

        find = places_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('normalized.address', '5312 LAUREL CANYON BLVD'),
                    ('normalized.city', 'VALLEY VILLAGE'),
                    ])
            )
        find.returns([place_1, place_2])

        event = OrderedDict([
                ('_id', '196872980394792'),
                ('facebook', OrderedDict([
                            ('venue', OrderedDict([
                                        ('id', '75054390179'),
                                        ('street', '5312 Laurel Canyon Blvd'),
                                        ('city', 'Valley Village'),
                                        ('longitude', -118.39647460),
                                        ('latitude', 34.1671240),
                                        ]),
                             ),
                            ]),
                 ),
                ('normalized', OrderedDict([
                            ('address', '5312 LAUREL CANYON BLVD'),
                            ('city', 'VALLEY VILLAGE'),
                            ('state', 'CA'),
                            ('country', 'US'),
                            ('zip5', '91607'),
                            ]),
                 ),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_place(
            event=event,
            place_ids=['10f20e75-8c9e-4ebc-879b-6104c42e0b6c'],
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 300),
                            ('place_id',
                             '10f20e75-8c9e-4ebc-879b-6104c42e0b6c'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.256867, 34.048416]),
                            ('search_type', 'place_ids'),
                            ('matched',
                             ['page',
                              'address',
                              'locality',
                              'region',
                              'postcode',
                              'country',
                              ],
                             )
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '5312 Laurel Canyon Blvd'),
                            ('country', 'US'),
                            ('locality', 'Valley Village'),
                            ('name', 'The Eclectic Company Theatre'),
                            ('postcode', '91607'),
                            ('region', 'CA'),
                            ('latitude', 34.048416),
                            ('longitude', -118.256867),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_venue_simple(self):
        facebook = OrderedDict([
                ('location', 'Playhouse'),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('place_id', '226680217397995'),
                            ('source', 'facebook'),
                            ('location', [-118.331231, 34.101593]),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_venue_onwer_name(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('place_id', '226680217397995'),
                            ('source', 'facebook'),
                            ('location', [-118.331231, 34.101593]),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_venue_state(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ('state', 'CA'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('place_id', '226680217397995'),
                            ('source', 'facebook'),
                            ('location', [-118.331231, 34.101593]),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('region', 'CA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_venue_country(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ('country', 'United States'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        expected = OrderedDict([
                ('ubernear', OrderedDict([
                            ('place_id', '226680217397995'),
                            ('source', 'facebook'),
                            ('location', [-118.331231, 34.101593]),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('country', 'USA'),
                            ]),
                 ),
                ])

        eq(expected, match)

    @fudge.with_fakes
    def test_match_with_venue_invalid_address(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', ''),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Event 226680217397995 has invalid address or locality. '
            'Skipping.'
            )

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_venue_invalid_locality(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', ''),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Event 226680217397995 has invalid address or locality. '
            'Skipping.'
            )

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_venue_invalid_latitude(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Event 226680217397995 has invalid latitude or longitude. '
            'Skipping.'
            )

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_venue_invalid_longitude(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Event 226680217397995 has invalid latitude or longitude. '
            'Skipping.'
            )

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_venue_latitude_little_precision(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.1212),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Event 226680217397995 has latitude or longitude '
            'with little precision. Skipping.'
            )

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_match_with_venue_longitude_little_precision(self):
        facebook = OrderedDict([
                ('owner', OrderedDict([
                            ('name', 'Playhouse'),
                            ]),
                 ),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.1223455),
                            ('longitude', -118.3323),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        debug = fake_log.expects('debug')
        debug.with_args(
            'Event 226680217397995 has latitude or longitude '
            'with little precision. Skipping.'
            )

        match = event_location._match_with_venue(
            event=event,
            _log=fake_log,
            )

        eq(None, match)

    @fudge.with_fakes
    def test_locate_match_with_place_simple(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query_parts = [
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_completed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_failed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            ]
        query = OrderedDict([
                ('$and', query_parts),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

        ubernear_1 = OrderedDict([
                ('place_ids', ['cb036268-2ba8-47db-906c-ca3b66d4da73']),
                ])
        ubernear_2 = OrderedDict([
                ('place_ids', ['cb036268-2ba8-47db-906c-ca3b66d4da73']),
                ])
        event_1 = OrderedDict([
                ('_id', '226680217397995'),
                ('ubernear', ubernear_1),
                ])
        event_2 = OrderedDict([
                ('_id', '267558763278075'),
                ('ubernear', ubernear_2),
                ])
        events = [event_1, event_2]

        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        update = events_coll.expects('update')
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set',
                     OrderedDict([
                                ('match.ubernear.score', 100),
                                ('match.ubernear.place_id',
                                 'cb036268-2ba8-47db-906c-ca3b66d4da73'
                                 ),
                                ('match.ubernear.source', 'factual'),
                                ('match.ubernear.location',
                                 [-118.331231, 34.101593],
                                 ),
                                ('match.ubernear.matched', ['page']),
                                ('match.place.address',
                                 '6506 Hollywood Blvd',
                                 ),
                                ('match.place.country', 'US'),
                                ('match.place.latitude', 34.101593),
                                ('match.place.longitude', -118.331231),
                                ('match.place.locality', 'Los Angeles'),
                                ('match.place.name', 'Playhouse'),
                                ('match.place.postcode', '90028'),
                                ('match.place.region', 'CA'),
                                ('ubernear.match_completed',
                                 datetime(2012, 5, 22, 3, 35, 8),
                                 ),
                                ]),
                     )]),
            upsert=True,
            safe=True,
            )

        update = events_coll.next_call('update')
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', OrderedDict([
                                ('match.ubernear.score', 100),
                                ('match.ubernear.place_id',
                                 'cca1d8d3-83ef-44d8-876c-23592b14f6e4',
                                 ),
                                ('match.ubernear.source', 'factual'),
                                ('match.ubernear.location',
                                 [-118.396004, 34.167198],
                                 ),
                                ('match.ubernear.matched', ['coord']),
                                ('match.ubernear.distance',
                                 44.07166072513344,
                                 ),
                                ('match.place.address', '320 E 2nd St'),
                                ('match.place.country', 'US'),
                                ('match.place.locality', 'Los Angeles'),
                                ('match.place.name',
                                 'Rafu California Patrol',
                                 ),
                                ('match.place.postcode', '90012'),
                                ('match.place.region', 'CA'),
                                ('match.place.latitude', 34.167198),
                                ('match.place.longitude', -118.396004),
                                ('ubernear.match_completed',
                                 datetime(2012, 5, 22, 3, 35, 8),
                                 ),
                                ]),
                     )]),
            upsert=True,
            safe=True,
            )

        find = events_coll.next_call('find')
        or_query = OrderedDict([
                ('$or', [
                        OrderedDict([
                                ('facebook.location', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.owner.name', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('ubernear.match_completed',
                                 OrderedDict([
                                            ('$exists', False),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('ubernear.match_failed',
                                 'No place match',
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.latitude',
                                 OrderedDict([
                                            ('$exists', True)
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.longitude',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.street',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.city',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        or_query,
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        info = fake_log.expects('info')
        info.with_args(
            'Matching 2 events',
            )

        places_coll = fudge.Fake('places_coll')
        database = fudge.Fake('database')

        fake_match_with_place = fudge.Fake(
            'match_with_place',
            callable=True,
            )
        fake_match_with_place.with_args(
            event=event_1,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            )
        match_1 = OrderedDict([
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
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('country', 'US'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('postcode', '90028'),
                            ('region', 'CA'),
                            ]),
                 ),
                ])
        fake_match_with_place.returns(match_1)

        fake_match_with_place.next_call()
        fake_match_with_place.with_args(
            event=event_2,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            )
        match_2 = OrderedDict([
                ('ubernear', OrderedDict([
                            ('score', 100),
                            ('place_id',
                             'cca1d8d3-83ef-44d8-876c-23592b14f6e4'
                             ),
                            ('source', 'factual'),
                            ('location', [-118.396004, 34.167198]),
                            ('matched', ['coord']),
                            ('distance', 44.07166072513344),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '320 E 2nd St'),
                            ('country', 'US'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('postcode', '90012'),
                            ('region', 'CA'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])
        fake_match_with_place.returns(match_2)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('utcnow')
        utcnow.returns(datetime(2012, 5, 22, 3, 35, 8))

        fake_match_with_venue = fudge.Fake('_match_with_venue')

        found_work = event_location.locate(
            events_coll=events_coll,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            _datetime=fake_datetime,
            _match_with_place_fn=fake_match_with_place,
            _match_with_venue_fn=fake_match_with_venue,
            )

        eq(found_work, True)

    @fudge.with_fakes
    def test_locate_match_with_venue_simple(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query_parts = [
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_completed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_failed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            ]
        query = OrderedDict([
                ('$and', query_parts),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        find = events_coll.next_call('find')
        or_query = OrderedDict([
                ('$or', [
                        OrderedDict([
                                ('facebook.location', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.owner.name', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('ubernear.match_completed',
                                 OrderedDict([
                                            ('$exists', False),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('ubernear.match_failed',
                                 'No place match',
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.latitude',
                                 OrderedDict([
                                            ('$exists', True)
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.longitude',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.street',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.city',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        or_query,
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

        facebook_1 = OrderedDict([
                ('location', 'Playhouse'),
                ('venue', OrderedDict([
                            ('street', '6506 Hollywood Blvd'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        facebook_2 = OrderedDict([
                ('location', 'Rafu California Patrol'),
                ('venue', OrderedDict([
                            ('street', '320 E 2nd St'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event_1 = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook_1),
                ])
        event_2 = OrderedDict([
                ('_id', '267558763278075'),
                ('facebook', facebook_2),
                ])
        events = [event_1, event_2]

        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        update = events_coll.expects('update')
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set',
                     OrderedDict([
                                ('match.ubernear.place_id',
                                 '226680217397995',
                                 ),
                                ('match.ubernear.source', 'facebook'),
                                ('match.ubernear.location',
                                 [-118.331231, 34.101593],
                                 ),
                                ('match.place.address',
                                 '6506 Hollywood Blvd',
                                 ),
                                ('match.place.locality', 'Los Angeles'),
                                ('match.place.name', 'Playhouse'),
                                ('match.place.latitude', 34.101593),
                                ('match.place.longitude', -118.331231),
                                ('ubernear.match_completed',
                                 datetime(2012, 5, 22, 3, 35, 8),
                                 ),
                                ]),
                     )]),
            upsert=True,
            safe=True,
            )

        update = events_coll.next_call('update')
        update.with_args(
            OrderedDict([
                    ('_id', '267558763278075'),
                    ]),
            OrderedDict([
                    ('$set', OrderedDict([
                                ('match.ubernear.place_id',
                                 '267558763278075',
                                 ),
                                ('match.ubernear.source', 'facebook'),
                                ('match.ubernear.location',
                                 [-118.396004, 34.167198],
                                 ),
                                ('match.place.address', '320 E 2Nd St'),
                                ('match.place.locality', 'Los Angeles'),
                                ('match.place.name',
                                 'Rafu California Patrol',
                                 ),
                                ('match.place.latitude', 34.167198),
                                ('match.place.longitude', -118.396004),
                                ('ubernear.match_completed',
                                 datetime(2012, 5, 22, 3, 35, 8),
                                 ),
                                ]),
                     )]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        info = fake_log.expects('info')
        info.with_args(
            'Resolving 2 venues',
            )

        places_coll = fudge.Fake('places_coll')
        database = fudge.Fake('database')

        fake_match_with_venue = fudge.Fake(
            'match_with_venue',
            callable=True,
            )
        fake_match_with_venue.with_args(
            event=event_1,
            _log=fake_log
            )
        match_1 = OrderedDict([
                ('ubernear', OrderedDict([
                            ('place_id', '226680217397995'),
                            ('source', 'facebook'),
                            ('location', [-118.331231, 34.101593]),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '6506 Hollywood Blvd'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Playhouse'),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ]),
                 ),
                ])
        fake_match_with_venue.returns(match_1)

        fake_match_with_venue.next_call()
        fake_match_with_venue.with_args(
            event=event_2,
            _log=fake_log,
            )
        match_2 = OrderedDict([
                ('ubernear', OrderedDict([
                            ('place_id', '267558763278075'),
                            ('source', 'facebook'),
                            ('location', [-118.396004, 34.167198]),
                            ]),
                 ),
                ('place', OrderedDict([
                            ('address', '320 E 2Nd St'),
                            ('locality', 'Los Angeles'),
                            ('name', 'Rafu California Patrol'),
                            ('latitude', 34.167198),
                            ('longitude', -118.396004),
                            ]),
                 ),
                ])
        fake_match_with_venue.returns(match_2)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('utcnow')
        utcnow.returns(datetime(2012, 5, 22, 3, 35, 8))

        fake_match_with_place = fudge.Fake('match_with_place')

        found_work = event_location.locate(
            events_coll=events_coll,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            _datetime=fake_datetime,
            _match_with_place_fn=fake_match_with_place,
            _match_with_venue_fn=fake_match_with_venue,
            )

        eq(found_work, True)

    @fudge.with_fakes
    def test_locate_match_with_place_none(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query_parts = [
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_completed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_failed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            ]
        query = OrderedDict([
                ('$and', query_parts),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

        ubernear = OrderedDict([
                ('place_ids', ['cb036268-2ba8-47db-906c-ca3b66d4da73']),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('ubernear', ubernear),
                ])
        events = [event]

        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        update = events_coll.expects('update')
        update.with_args(
            OrderedDict([
                    ('_id', '226680217397995'),
                    ]),
            OrderedDict([
                    ('$set', OrderedDict([
                                ('ubernear.match_failed',
                                 'No place match',
                                 ),
                                ]),
                     )]),
            upsert=True,
            safe=True,
            )

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        find = events_coll.next_call('find')
        or_query = OrderedDict([
                ('$or', [
                        OrderedDict([
                                ('facebook.location', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.owner.name', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('ubernear.match_completed',
                                 OrderedDict([
                                            ('$exists', False),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('ubernear.match_failed',
                                 'No place match',
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.latitude',
                                 OrderedDict([
                                            ('$exists', True)
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.longitude',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.street',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.city',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        or_query,
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        info = fake_log.expects('info')
        info.with_args(
            'Matching 1 event',
            )

        places_coll = fudge.Fake('places_coll')
        database = fudge.Fake('database')

        fake_match_with_place = fudge.Fake(
            'match_with_place',
            callable=True,
            )
        fake_match_with_place.with_args(
            event=event,
            place_ids=['cb036268-2ba8-47db-906c-ca3b66d4da73'],
            places_coll=places_coll,
            database=database,
            )
        fake_match_with_place.returns(None)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('utcnow')
        utcnow.returns(datetime(2012, 5, 22, 3, 35, 8))

        fake_match_with_venue = fudge.Fake('match_with_venue')

        found_work = event_location.locate(
            events_coll=events_coll,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            _datetime=fake_datetime,
            _match_with_place_fn=fake_match_with_place,
            _match_with_venue_fn=fake_match_with_venue,
            )

        eq(found_work, True)

    @fudge.with_fakes
    def test_locate_match_with_venue_none(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query_parts = [
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_completed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_failed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            ]
        query = OrderedDict([
                ('$and', query_parts),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        find = events_coll.next_call('find')
        or_query = OrderedDict([
                ('$or', [
                        OrderedDict([
                                ('facebook.location', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.owner.name', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('ubernear.match_completed',
                                 OrderedDict([
                                            ('$exists', False),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('ubernear.match_failed',
                                 'No place match',
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.latitude',
                                 OrderedDict([
                                            ('$exists', True)
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.longitude',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.street',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.city',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        or_query,
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

        facebook = OrderedDict([
                ('location', 'Playhouse'),
                ('venue', OrderedDict([
                            ('street', ''),
                            ('latitude', 34.101593),
                            ('longitude', -118.331231),
                            ('city', 'Los Angeles'),
                            ]),
                 ),
                ])
        event = OrderedDict([
                ('_id', '226680217397995'),
                ('facebook', facebook),
                ])
        events = [event]

        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        fake_log = fudge.Fake('log')
        fake_log.remember_order()

        info = fake_log.expects('info')
        info.with_args(
            'Resolving 1 venue',
            )

        fake_match_with_venue = fudge.Fake(
            'match_with_venue',
            callable=True,
            )
        fake_match_with_venue.with_args(
            event=event,
            _log=fake_log,
            )
        fake_match_with_venue.returns(None)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('utcnow')
        utcnow.returns(datetime(2012, 5, 22, 3, 35, 8))

        places_coll = fudge.Fake('places_coll')
        database = fudge.Fake('database')
        fake_match_with_place = fudge.Fake('match_with_place')

        found_work = event_location.locate(
            events_coll=events_coll,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            _datetime=fake_datetime,
            _match_with_place_fn=fake_match_with_place,
            _match_with_venue_fn=fake_match_with_venue,
            )

        eq(found_work, True)

    @fudge.with_fakes
    def test_locate_process_all(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        find.with_args(
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        find = events_coll.next_call('find')
        find.with_args(
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('utcnow')
        utcnow.returns(datetime(2012, 5, 22, 3, 35, 8))

        fake_log = fudge.Fake('log')
        places_coll = fudge.Fake('places_coll')
        database = fudge.Fake('database')
        fake_match_with_place = fudge.Fake('match_with_place')
        fake_match_with_venue = fudge.Fake('match_with_venue')

        found_work = event_location.locate(
            events_coll=events_coll,
            places_coll=places_coll,
            database=database,
            process_all=True,
            _log=fake_log,
            _match_with_place_fn=fake_match_with_place,
            _match_with_venue_fn=fake_match_with_venue,
            _datetime=fake_datetime,
            )

        eq(found_work, False)

    @fudge.with_fakes
    def test_locate_no_work(self):
        events_coll = fudge.Fake('events_coll')
        events_coll.remember_order()

        find = events_coll.expects('find')
        query_parts = [
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_completed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            OrderedDict([
                    ('ubernear.match_failed',
                     OrderedDict([
                                ('$exists', False),
                                ]),
                     ),
                    ]),
            ]
        query = OrderedDict([
                ('$and', query_parts),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

        events = []

        fake_cursor = FakeCursor(events)
        find.returns(fake_cursor)

        find = events_coll.next_call('find')
        or_query = OrderedDict([
                ('$or', [
                        OrderedDict([
                                ('facebook.location', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.owner.name', OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        ],
                 ),
                ])
        query = OrderedDict([
                ('$and', [
                        OrderedDict([
                                ('ubernear.match_completed',
                                 OrderedDict([
                                            ('$exists', False),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('ubernear.match_failed',
                                 'No place match',
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.latitude',
                                 OrderedDict([
                                            ('$exists', True)
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.longitude',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.street',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        OrderedDict([
                                ('facebook.venue.city',
                                 OrderedDict([
                                            ('$exists', True),
                                            ]),
                                 ),
                                ]),
                        or_query,
                        ],
                 ),
                ])
        find.with_args(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
        fake_cursor = FakeCursor([])
        find.returns(fake_cursor)

        fake_datetime = fudge.Fake('datetime')
        utcnow = fake_datetime.expects('utcnow')
        utcnow.returns(datetime(2012, 5, 22, 3, 35, 8))

        fake_log = fudge.Fake('log')
        places_coll = fudge.Fake('places_coll')
        database = fudge.Fake('database')
        fake_match_with_place = fudge.Fake('match_with_place')
        fake_match_with_venue = fudge.Fake('match_with_venue')

        found_work = event_location.locate(
            events_coll=events_coll,
            places_coll=places_coll,
            database=database,
            _log=fake_log,
            _datetime=fake_datetime,
            _match_with_place_fn=fake_match_with_place,
            _match_with_venue_fn=fake_match_with_venue,
            )

        eq(found_work, False)
