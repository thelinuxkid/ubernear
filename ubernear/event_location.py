import itertools
import bson
import logging
import pymongo

from collections import OrderedDict
from datetime import datetime
from decimal import Decimal

from ubernear.util import mongo
from ubernear.util import (
    DefaultOrderedDict,
    )
from ubernear.util.address import (
    streets_equal,
    cities_equal,
    states_equal,
    countries_equal,
    zipcodes_equal,
    normalize_string,
    normalize_street,
    normalize_city,
    normalize_state,
    normalize_country,
    )

log = logging.getLogger(__name__)

# In meters
earth_radius = 6371000
# Look for places nearby that are not farther than this distance
max_meters = 100
# Angle, in radians, of an arc with length max_meters
# for a sphere with radius earth_radius
max_angle = 0.000015696

place_fields = [
    'address',
    'country',
    'latitude',
    'longitude',
    'locality',
    'name',
    'postcode',
    'region',
    ]
required_place_fields = [
    'address',
    'locality',
    'name',
    'latitude',
    'longitude',
    ]
venue_fields = [
    'coords',
    'page_id',
    'address',
    'locality',
    'region',
    'country',
    'names',
    'postcode',
    ]
match_fields = [
    'page',
    'coord',
    'address',
    'locality',
    'region',
    'postcode',
    'country',
    'name',
    ]
normalized_fields = OrderedDict([
        ('address', 'address'),
        ('country', 'country'),
        ('city', 'locality'),
        ('name', 'name'),
        ('zip5', 'postcode'),
        ('state', 'region'),
        ])

def _missing_field(address):
    for field in required_place_fields:
        if field not in address.keys():
            return field

def _highest_match(matches):
    highest = None
    for place_id in matches.keys():
        score = matches[place_id]['ubernear']['score']
        # Sacrifice compound conditional statements for readability
        if highest is None:
            if score >= 100:
                highest = place_id
            else:
                continue
        else:
            if score > matches[highest]['ubernear']['score']:
                highest = place_id
            elif score < matches[highest]['ubernear']['score']:
                continue
            else:
                # Break tie
                # TODO There is probably a more elegant way to do this
                old = matches[highest]['ubernear']['matched']
                new = matches[place_id]['ubernear']['matched']

                # Page match weighs heaviest
                if new['page'] and not old['page']:
                    highest = place_id
                    continue
                elif not new['page'] and old['page']:
                    continue

                # Address and postcode match weighs next heaviest
                if (
                    (new['address'] and new['postcode'])
                    and not
                    (old['address'] and old['postcode'])
                    ):
                    highest = place_id
                    continue
                elif (not
                      (new['address'] and new['postcode'])
                      and
                      (old['address'] and old['postcode'])
                      ):
                    continue

                # Address and locality match weighs next heaviest
                if (
                    (new['address'] and new['locality'])
                    and not
                    (old['address'] and old['locality'])
                    ):
                    highest = place_id
                    continue
                elif (not
                      (new['address'] and new['locality'])
                      and
                      (old['address'] and old['locality'])
                      ):
                    continue

                # Coord and address match weighs next heaviest
                if (
                    (new['coord'] and new['address'])
                    and not
                    (old['coord'] and old['address'])
                    ):
                    highest = place_id
                    continue
                elif (not
                      (new['coord'] and new['address'])
                      and
                      (old['coord'] and old['address'])
                      ):
                    continue

                # Coord and name match weighs next heaviest
                if (
                    (new['coord'] and new['name'])
                    and not
                    (old['coord'] and old['name'])
                    ):
                    highest = place_id
                    continue
                elif (not
                      (new['coord'] and new['name'])
                      and
                      (old['coord'] and old['name'])
                      ):
                    continue

                # Address and name match weighs next heaviest
                # Break tie with region
                if (
                    (new['address'] and new['name'] and new['region'])
                    and not
                    (old['address'] and old['name'] and old['region'])
                    ):
                    highest = place_id
                    continue
                elif (not
                      (new['address'] and new['name'] and new['region'])
                      and
                      (old['address'] and old['name'] and old['region'])
                      ):
                    continue

                # Break next tie with country
                if (
                    (new['address'] and new['name'] and new['country'])
                    and not
                    (old['address'] and old['name'] and old['country'])
                    ):
                    highest = place_id
                    continue
                elif (not
                      (new['address'] and new['name'] and new['country'])
                      and
                      (old['address'] and old['name'] and old['country'])
                      ):
                    continue

    return highest

def _event_venue(event):
    event_venue = OrderedDict(
        [(field, None) for field in venue_fields]
        )
    event_venue['names'] = []

    facebook_ = event['facebook']
    venue = facebook_.get('venue', None)
    if venue:
        if 'latitude' in venue and 'longitude' in venue:
            lng = venue['longitude']
            lat = venue['latitude']
            event_venue['coords'] = [lng, lat]
        if 'id' in venue:
            event_venue['page_id'] = venue['id']
        if 'street' in venue:
            event_venue['address'] = venue['street']
        if 'city' in venue:
            event_venue['locality'] = venue['city']
        if 'state' in venue:
            event_venue['region'] = venue['state']
        if 'country' in venue:
            event_venue['country'] = venue['country']
        # Have never come across a venue with a zip code

    # Always prefer normalized adresses. Override any venue values.
    if 'normalized' in event:
        # TODO add check for required normalized fields
        norm_venue = event['normalized']
        event_venue['address'] = norm_venue['address']
        event_venue['locality'] = norm_venue['city']
        event_venue['region'] = norm_venue['state']
        event_venue['country'] = norm_venue['country']
        event_venue['postcode'] = norm_venue['zip5']

    if 'location' in facebook_:
        event_venue['names'].append(facebook_['location'])
    if 'owner' in facebook_ and 'name' in facebook_['owner']:
        event_venue['names'].append(facebook_['owner']['name'])

    return event_venue

def _place_info(
    place,
    _log,
    ):
    place_info = OrderedDict([
            ('_id', place['_id']),
            ('page_ids', None),
            ('source', place['ubernear']['source']),
            ])

    if 'location' in place['ubernear']:
        place_info['coords'] = place['ubernear']['location']
    else:
        _log.error(
            'Place {place_id} has no indexed coordinates. '
            'Skipping place'.format(
                place_id=place['_id'],
                )
            )
        return None

    lng, lat = place_info['coords']
    # Always prefer normalized addresses
    if 'normalized' in place:
        # TODO add check for required normalized fields
        address = OrderedDict([
                (info, place['normalized'][norm].title())
                for norm, info in normalized_fields.items()
             ])
        address['region'] = address['region'].upper()
        address['country'] = address['country'].upper()
    else:
        address = OrderedDict([
                (field, place['info'][field])
                for field in place_fields
                if field in place['info']
                ])
    address['latitude'] = lat
    address['longitude'] = lng
    missing = _missing_field(address)
    if missing is not None:
        _log.error(
            'Place {_id} is missing field "{field}". '
            'Skipping place'.format(
                _id=place['_id'],
                field=missing,
                )
            )
        return None

    place_info['address'] = address

    if 'facebook' in place and 'pages' in place['facebook']:
        pages = place['facebook']['pages']
        place_info['page_ids'] = [page['id'] for page in pages]

    return place_info

def _nearby_places(
    venue,
    event_id,
    places_coll,
    database,
    _log,
    ):
    if venue['coords']:
        try:
            res = database.command(
                bson.SON(
                    OrderedDict([
                            ('geoNear', places_coll.name),
                            ('near', venue['coords']),
                            ])
                    ),
                spherical=True,
                maxDistance=max_angle,
                distanceMultiplier=earth_radius,
                )
        except pymongo.errors.OperationFailure, e:
            _log.error(
                'GeoNear search returned error "{error}" for event '
                '{event_id}'.format(
                    error=str(e),
                    event_id=event_id,
                    )
                )
        else:
            if res['ok'] != 1.0:
                _log.error(
                    'GeoNear search failed for event '
                    '{event_id}'.format(
                        event_id=event_id,
                        )
                    )
            else:
                for result in res['results']:
                    place_info = _place_info(
                        result['obj'],
                        _log,
                        )
                    if place_info is None:
                        continue

                    distance =result['dis']
                    if distance > max_meters:
                        _log.error(
                            'GeoNear search returned distance {dis} '
                            'which is greater than {max_meters} meters '
                            'for event {event_id} and place '
                            '{place_id}. Skipping place.'.format(
                                dis=distance,
                                max_meters=max_meters,
                                event_id=event_id,
                                place_id=place_info['_id'],
                                )
                            )
                        continue

                    yield OrderedDict([
                            ('info', place_info),
                            ('type', 'coordinate'),
                            ('distance', distance),
                            ])

def _normalized_places(
    event,
    places_coll,
    _log,
    ):
    if 'normalized' in event:
        norm_event = event['normalized']
        cursor = places_coll.find(
            OrderedDict([
                    ('normalized.address', norm_event['address']),
                    ('normalized.city', norm_event['city']),
                    ]),
            )
        for place in cursor:
            place_info = _place_info(
                place,
                _log,
                )
            if place_info is None:
                continue

            yield OrderedDict([
                    ('info', place_info),
                    ('type', 'normalized'),
                    ])

def _place_ids_places(
    place_ids,
    places_coll,
    _log,
    ):
    for place_id in place_ids:
        place = places_coll.find_one(
            OrderedDict([('_id', place_id)]),
            )
        place_info = _place_info(
            place,
            _log,
            )
        if place_info is None:
            continue

        yield OrderedDict([
                ('info', place_info),
                ('type', 'place_ids'),
                ])

def _match_with_place(
    event,
    place_ids,
    places_coll,
    database,
    _log=None,
    ):
    if _log is None:
        _log = log

    venue = _event_venue(event)
    matches = DefaultOrderedDict(OrderedDict)
    seen = []
    places = itertools.chain(
        # Always check nearby places first so that coord information
        # is not skipped if the same place is found twice
        _nearby_places(
            venue=venue,
            event_id=event['_id'],
            places_coll=places_coll,
            database=database,
            _log=_log,
            ),
        _place_ids_places(
            place_ids=place_ids,
            places_coll=places_coll,
            _log=_log,
            ),
        _normalized_places(
            event=event,
            places_coll=places_coll,
            _log=_log,
            ),
        )
    for place in places:
        place_info = place['info']
        place_id = place_info['_id']
        search_type = place['type']
        if place_id in seen:
            continue
        seen.append(place_id)

        matched = OrderedDict(
            [(item, False) for item in match_fields]
            )
        distance = None
        venue_name = None
        score = 0

        if 'distance' in place:
            distance = place['distance']
            matched['coord'] = True

        if venue['address']:
            matched['address'] = streets_equal(
                venue['address'],
                place_info['address']['address'],
                )

        if venue['locality']:
            matched['locality'] = cities_equal(
                venue['locality'],
                place_info['address']['locality'],
                )

        for name in venue['names']:
            name_ = normalize_string(
                name,
                )
            address_name = normalize_string(
                place_info['address']['name'],
                )
            match_ = (address_name in name_
                      or
                      name_ in address_name
                      )
            if match_:
                venue_name = name
                matched['name'] = match_
                break

        if venue['region'] and 'region' in place_info['address']:
            matched['region'] = states_equal(
                venue['region'],
                place_info['address']['region'],
                )

        if venue['country'] and 'country' in place_info['address']:
            matched['country'] = countries_equal(
                venue['country'],
                place_info['address']['country'],
                )

        if venue['postcode'] and 'postcode' in place_info['address']:
            matched['postcode'] = zipcodes_equal(
                venue['postcode'],
                place_info['address']['postcode'],
                )

        if place_info['page_ids'] and venue['page_id']:
            if venue['page_id'] in place_info['page_ids']:
                matched['page'] = True

        # A score of 100 or more denotes a successful match
        # TODO places from place_ids should be probably be
        # treated differently since there is a higher chance
        # they will be a match
        if matched['coord'] and matched['name']:
            score += 100
        if matched['coord'] and matched['address']:
            score += 100
        if matched['address'] and matched['name']:
            score += 100
        if matched['address'] and matched['locality']:
            score += 100
        if matched['postcode'] and matched['address']:
            score += 100
        if matched['page']:
            score += 100

        current = matches[place_id]
        current['ubernear'] = OrderedDict([
                ('score', score),
                ('place_id', place_id),
                ('source', place_info['source']),
                ('location', place_info['coords']),
                ('search_type', search_type),
                ])
        current['ubernear']['matched'] = matched
        if distance is not None:
            current['ubernear']['distance'] = distance

        # Store address information in the match so no places_coll
        # look-ups are necessary when serving the event
        current['place'] = place_info['address']

        # Some places will match on the address but the name
        # of the places are actually different, e.g., two
        # businesses with the same address but different unit
        # numbers. So, it's safer to use the facebook venue name
        if venue_name:
            current['place']['name'] = venue_name
        elif venue['names']:
            current['place']['name'] = venue['names'][0]

    highest = _highest_match(matches)

    match = None
    if highest is not None:
        match = matches[highest]
        # Store a list of only those parts that have matched
        highest_matched = [
            k
            for k,v in
            match['ubernear']['matched'].iteritems()
            if v
            ]
        match['ubernear']['matched'] = highest_matched

    return match

def _match_with_venue(
    event,
    _log=None,
    ):
    if _log is None:
        _log = log

    facebook = event['facebook']
    name = facebook.get('location')
    if name is None:
        name = facebook['owner']['name']

    venue = facebook['venue']
    address = normalize_street(venue['street'])
    locality = normalize_city(venue['city'])
    region = normalize_state(venue.get('state', ''))
    country = normalize_country(venue.get('country', ''))

    if not address or not locality:
        _log.debug(
            'Event {event_id} has invalid address or locality. '
            'Skipping.'.format(
                event_id=event['_id'],
                )
            )
        return None

    address = address.title()
    locality = locality.title()

    latitude = venue['latitude']
    longitude = venue['longitude']
    # coordinates of type int are too ambigious to be considered
    # good
    if type(latitude) is not float or type(longitude) is not float:
        _log.debug(
            'Event {event_id} has invalid latitude or longitude. '
            'Skipping.'.format(
                event_id=event['_id'],
                )
            )
        return None

    # coordinates with little precision are too ambigious to be
    # considered good
    lat_precision = Decimal(repr(latitude))
    lng_precision = Decimal(repr(longitude))
    lat_precision = lat_precision.as_tuple().exponent
    lng_precision = lng_precision.as_tuple().exponent
    if lat_precision > -5 or lng_precision > -5:
        _log.debug(
            'Event {event_id} has latitude or longitude with '
            'little precision. Skipping.'.format(
                event_id=event['_id'],
                )
            )
        return None

    match = OrderedDict([
            ('ubernear', OrderedDict([
                        ('place_id', event['_id']),
                        ('source', 'facebook'),
                        ('location', [longitude, latitude]),
                        ]),
             ),
            ('place', OrderedDict([
                        ('address', address),
                        ('locality', locality),
                        ('name', name),
                        ('latitude', latitude),
                        ('longitude', longitude),
                        ]),
             ),
            ])

    if region:
        region = region.upper()
        match['place']['region'] = region
    if country:
        country = country.upper()
        match['place']['country'] = country

    return match

def locate(
    events_coll,
    places_coll,
    database,
    process_all=False,
    _log=None,
    _datetime=None,
    _match_with_place_fn=None,
    _match_with_venue_fn=None,
    ):
    if _log is None:
        _log = log
    if _datetime is None:
        _datetime = datetime
    if _match_with_place_fn is None:
        _match_with_place_fn = _match_with_place
    if _match_with_venue_fn is None:
        _match_with_venue_fn = _match_with_venue

    now = _datetime.utcnow()

    if process_all:
        events = events_coll.find(
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
    else:
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
        events = events_coll.find(
            OrderedDict([
                    ('$and', query_parts),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

    count = events.count()
    if count != 0:
        _log.info(
            'Matching {count} event{s}'.format(
                count=count,
                s='' if count == 1 else 's',
                ),
            )

    found_work = False
    for event in events:
        found_work = True
        ubernear = event['ubernear']
        place_ids = ubernear.get('place_ids', [])
        place_ids = place_ids
        match = _match_with_place_fn(
            event=event,
            place_ids=place_ids,
            places_coll=places_coll,
            database=database,
            )

        if match is not None:
            save = OrderedDict([
                    ('match', match),
                    ('ubernear.match_completed', now),
                    ])
            mongo.save_no_replace(
                events_coll,
                _id=event['_id'],
                save=save,
                )
        else:
            save = OrderedDict([
                    ('ubernear.match_failed', 'No place match'),
                    ])
            mongo.save_no_replace(
                events_coll,
                _id=event['_id'],
                save=save,
                )

    if process_all:
        events = events_coll.find(
            OrderedDict([
                    ('ubernear.lookup_completed',
                     OrderedDict([
                                ('$exists', True),
                                ]),
                     ),
                    ]),
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )
    else:
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
                                            ('$exists', False)
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
        events = events_coll.find(
            query,
            sort=[('ubernear.fetched', pymongo.ASCENDING)],
            )

    count = events.count()
    if count != 0:
        _log.info(
            'Resolving {count} venue{s}'.format(
                count=count,
                s='' if count == 1 else 's',
                ),
            )

    for event in events:
        found_work = True
        match = _match_with_venue_fn(
            event=event,
            _log=_log,
            )
        if match is not None:
            save = OrderedDict([
                    ('match', match),
                    ('ubernear.match_completed', now),
                    ])
            mongo.save_no_replace(
                events_coll,
                _id=event['_id'],
                save=save,
                )

    return found_work
