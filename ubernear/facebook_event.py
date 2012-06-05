import logging
import pymongo

from collections import OrderedDict
from datetime import datetime, timedelta
from facepy.exceptions import FacepyError
from pyusps import address_information
from pygeocode import geocoder

from ubernear.util import mongo
from ubernear.util import (
    utc_from_iso8601,
    address as addr_util,
    )

log = logging.getLogger(__name__)

facebook_batch_size = 50
usps_batch_size = 5

def _mark_as_failed(
    events_coll,
    event_id,
    now,
    field,
    reason='',
    ):
    save = OrderedDict([
            ('ubernear', OrderedDict([
                        (field, OrderedDict([
                                    # If event is retried and it is
                                    # successful it would be useful
                                    # to know when it failed.
                                    ('when', now),
                                    ('reason', reason),
                                    ]),
                         ),
                        ]),
             ),
            ])

    mongo.save_no_replace(
        events_coll,
        _id=event_id,
        save=save,
        )

def _save_venues(
    events,
    events_coll,
    usps_id,
    now,
    ):
    # Don't waste a call to the USPS API
    if not events:
        return

    venues = [event['facebook']['venue'] for event in events]
    usps_venues = [
        OrderedDict([
                ('address', venue['street']),
                ('city', venue['city']),
                ('state', venue['state']),
                ])
        for venue in venues
        ]
    matches = address_information.verify(
        usps_id,
        *usps_venues
        )
    # TODO fugly
    if len(usps_venues) == 1:
        matches = [matches]
    for (event,match) in zip(events,matches):
        if isinstance(match, ValueError):
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='normalization_failed',
                reason=str(match),
                )
            continue

        match['country'] = 'US'
        save = OrderedDict([
            ('normalized', match),
            ('ubernear.normalization_completed', now),
            ('ubernear.normalization_source', 'usps'),
            ])

        log.debug(
            'Storing normalized venue for {event_id}'.format(
                event_id=event['_id'],
                )
            )
        mongo.save_no_replace(
            events_coll,
            _id=event['_id'],
            save=save,
            )

def _save_events(
    events,
    events_coll,
    graph,
    now,
    _log=None,
    ):
    if _log is None:
        _log = log

    # Don't waste a call to the Facebook Graph API
    if not events:
        return

    batch = [
        OrderedDict([
                ('method', 'GET'),
                ('relative_url', event['_id']),
                ])
        for event in events
        ]
    reponses = graph.batch(batch)

    for event,response in zip(events,reponses):
        if isinstance(response, FacepyError):
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='lookup_failed',
                reason=str(response),
                )
            continue
        # Event does not exist anymore
        if response is False:
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='lookup_failed',
                reason='False response',
                )
            continue
        if response is None:
            # None has special significance in mongodb searches
            # so use 'null' instead.
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='lookup_failed',
                reason='Null response',
                )
            continue

        # We seem to have a valid response but ids are different?
        if response['id'] != event['_id']:
            _log.error(
                'Facebook returned information for an event other than '
                '{event_id}. Skipping event.'.format(
                    event_id=event['_id'],
                    )
                )
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='lookup_failed',
                reason='Response id is different',
                )
            continue

        save = OrderedDict([
                ('facebook', response),
                ('ubernear', OrderedDict([
                            # Depending on where the event came from,
                            # the event source may not have already
                            # been set
                            ('source', 'facebook'),
                            ('lookup_completed', now),
                            ]),
                 ),
                ])

        # Skip responses without a start_time or end_time.
        # Sometimes the Graph API returns events without these
        if (
            'start_time' in save['facebook']
            and
            'end_time' in save['facebook']
            ):
            save['facebook']['start_time'] = utc_from_iso8601(
                save['facebook']['start_time'],
                naive=True,
                )
            save['facebook']['end_time'] = utc_from_iso8601(
                save['facebook']['end_time'],
                naive=True,
                )
        else:
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='lookup_failed',
                reason='Missing start_time or end_time',
                )
            continue
        if 'updated_time' in save['facebook']:
            save['facebook']['updated_time'] = utc_from_iso8601(
                save['facebook']['updated_time'],
                naive=True,
                )


        _log.debug(
            'Storing event {event_id}'.format(
                event_id=event['_id'],
                )
            )

        mongo.save_no_replace(
            events_coll,
            _id=event['_id'],
            save=save,
            )

def update_facebook(
    events_coll,
    graph,
    process_all=False,
    _log=None,
    _datetime=None,
    ):
    if _log is None:
        _log = log
    if _datetime is None:
        _datetime = datetime

    now = _datetime.utcnow()

    if process_all:
        events = events_coll.find()
    else:
        # The next three represent transitional graph API errors
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
        events = events_coll.find(
            OrderedDict([
                    ('$or', [match_query, and_query]),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

    count = events.count()
    if count != 0:
        _log.info(
            'Fetching {count} event{s}'.format(
                count=count,
                s='' if count == 1 else 's',
                ),
            )

    event_batch = []
    found_work = False
    # TODO This cursor may timeout if there are too many results
    for event in events:
        found_work = True
        event_batch.append(event)
        if len(event_batch) == facebook_batch_size:
            _save_events(
                events=event_batch,
                events_coll=events_coll,
                graph=graph,
                now=now,
                _log=_log,
                )
            event_batch = []

    _save_events(
        events=event_batch,
        events_coll=events_coll,
        graph=graph,
        now=now,
        _log=_log,
        )

    return found_work

def expire(
    events_coll,
    expired_coll,
    _datetime=None,
    ):
    if _datetime is None:
        _datetime = datetime

    last_week = _datetime.utcnow() - timedelta(days=7)
    end_parts = [
        # No guarantees in documentation that
        # $lt doesn't return rows where
        # the field doesn't exist
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
    # It seems facebook should return false instead of this error,
    # i.e., the id cannot be found. No bug report has been found to
    # confirm this although some reports suggest it.
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
    cursor = events_coll.find(
        OrderedDict([
                ('$or',
                 [end_query,
                  failed_query,
                  ]
                 ),
                ]),
        sort=[('facebook.end_time', pymongo.ASCENDING)],
        )

    for event in cursor:
        event_id = event.pop('_id')
        kwargs = OrderedDict([
            ('_id', event_id),
            ('save', event),
            ])

        ubernear = event['ubernear']
        place_ids = ubernear.get('place_ids')
        if place_ids is not None:
            # Add to a set of ubernear.place_ids
            kwargs['add_each'] = OrderedDict([
                    ('ubernear.place_ids', place_ids),
                    ])
            del ubernear['place_ids']

        mongo.save_no_replace(
            expired_coll,
            **kwargs
            )
        events_coll.remove(
            OrderedDict([
                    ('_id', event_id),
                    ])
            )

def update_venue(
    events_coll,
    usps_id,
    process_all,
    ):
    now = datetime.utcnow()

    if process_all:
        events = events_coll.find()
    else:
        completed_query = OrderedDict([
                ('ubernear.normalization_completed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        failed_query = OrderedDict([
                ('ubernear.normalization_failed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        lookup_query = OrderedDict([
                ('ubernear.lookup_completed',
                 OrderedDict([
                            ('$exists', True),
                            ]),
                 ),
                ])
        events = events_coll.find(
            OrderedDict([
                    ('$and',
                     [completed_query,
                      failed_query,
                      lookup_query,
                      ]
                     ),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

    count = events.count()
    if count != 0:
        log.info(
            'Normalizing {count} event{s}'.format(
                count=count,
                s='' if count == 1 else 's',
                ),
            )
    event_batch = []
    found_work = False
    # TODO This cursor may timeout if there are too many results
    for event in events:
        found_work = True
        # Don't send venues in the batch that can't be used
        # Check for missing values here instead of in the query
        # so it is explicitly known which events are not
        # eligible for normalization
        if not 'venue' in event['facebook']:
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='normalization_failed',
                reason='No venue',
                )
            continue
        venue = event['facebook']['venue']
        # The minimal requirements for the USPS API
        if (
            not 'street' in venue
            or not 'city' in venue
            or not 'state' in venue
            ):
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='normalization_failed',
                reason='No street, city or state',
                )
            continue
        # USPS doesn't take long names for states
        venue['state'] = addr_util.normalize_state(
            venue['state']
            )
        # Make sure it's a valid state abbreviation
        if venue['state'] not in addr_util.state_abbrev.keys():
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='normalization_failed',
                reason='Invalid state',
                )
            continue
        event_batch.append(event)
        if len(event_batch) == usps_batch_size:
            _save_venues(
                events=event_batch,
                events_coll=events_coll,
                usps_id=usps_id,
                now=now,
                )
            event_batch = []

    _save_venues(
        events=event_batch,
        events_coll=events_coll,
        usps_id=usps_id,
        now=now,
        )

    return found_work

def update_coordinate(
    events_coll,
    yahoo_id,
    process_all,
    ):
    now = datetime.utcnow()

    if process_all:
        events = events_coll.find()
    else:
        latitude_query = OrderedDict([
                ('facebook.venue.latitude',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        longitude_query = OrderedDict([
                ('facebook.venue.longitude',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        or_query = OrderedDict([
                ('$or',
                 [latitude_query,
                  longitude_query,
                  ]
                 ),
                ])
        failed_query = OrderedDict([
                ('ubernear.geocoding_failed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        completed_query = OrderedDict([
                ('ubernear.geocoding_completed',
                 OrderedDict([
                            ('$exists', False),
                            ]),
                 ),
                ])
        lookup_query = OrderedDict([
                ('ubernear.lookup_completed',
                 OrderedDict([
                            ('$exists', True),
                            ]),
                 ),
                ])
        query = OrderedDict([
                ('$and',
                 [or_query,
                  failed_query,
                  completed_query,
                  lookup_query,
                  ]
                 ),
                ])
        events = events_coll.find(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

    count = events.count()
    if count != 0:
        log.info(
            'Geocoding {count} event{s}'.format(
                count=count,
                s='' if count == 1 else 's',
                ),
            )
    found_work = OrderedDict([
            ('found_work', False),
            ('sleep', None),
            ])
    # TODO This cursor may timeout if there are too many results
    for event in events:
        found_work['found_work'] = True
        # Check for missing values here instead of in the query
        # so it is explicitly known which events are not
        # eligible for geocoding
        if not 'venue' in event['facebook']:
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='geocoding_failed',
                reason='No venue',
                )
            continue
        venue = event['facebook']['venue']
        # The minimal requirements for geocoding
        if 'normalized' in event:
            address = event['normalized']['address']
            city = event['normalized']['city']
        elif (
            not 'street' in venue
            or not 'city' in venue
            ):
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='geocoding_failed',
                reason='No street or city',
                )
            continue
        else:
            address = venue['street']
            city = venue['city']
        request = '{address},{city}'.format(
            address=address.encode('utf-8'),
            city=city.encode('utf-8'),
            )
        try:
            # TODO figure out which error corresponds to the
            # rate limit reached and return the number of hours
            # to sleep
            response = geocoder.geocode_yahoo(request, yahoo_id)
        except geocoder.GeocoderAmbiguousResultError, e:
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='geocoding_failed',
                reason=str(e),
                )
            continue
        if response is None:
            _mark_as_failed(
                events_coll=events_coll,
                event_id=event['_id'],
                now=now,
                field='geocoding_failed',
                reason='Null response',
                )
            continue

        save = OrderedDict([
            ('facebook.venue.latitude', response['lat']),
            ('facebook.venue.longitude', response['lng']),
            ('ubernear.geocoding_completed', now),
            ('ubernear.geocoding_source', 'yahoo'),
            ])
        log.debug(
            'Storing coordinates for {event_id}'.format(
                event_id=event['_id'],
                )
            )
        mongo.save_no_replace(
            events_coll,
            _id=event['_id'],
            save=save,
            )

    return found_work
