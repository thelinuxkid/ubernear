import csv
import optparse
import logging
import pymongo

from collections import defaultdict
from pyusps import address_information

from ubernear.util import signal_handler
from ubernear.util.config import (
    collections,
    config_parser,
    absolute_path,
    )
from ubernear.util import mongo

log = logging.getLogger(__name__)

@signal_handler
def main():
    parser = optparse.OptionParser(
        usage='%prog [OPTS]',
        )
    parser.add_option(
        '-v', '--verbose',
        help='Verbose mode [default %default]',
        action="store_true", dest="verbose"
        )
    parser.add_option(
        '--csv',
        help='Path to the CSV file containing the places to import',
        metavar='PATH',
        )
    parser.add_option(
        '--config',
        help=('Path to the config file with information on how to '
              'import places'
              ),
        metavar='PATH',
        )
    parser.add_option(
        '--db-config',
        help=('Path the to file with information on how to '
              'retrieve and store data in the database'
              ),
        metavar='PATH',
        )
    parser.set_defaults(
        verbose=False,
        )

    options, args = parser.parse_args()
    if args:
        parser.error('Wrong number of arguments.')

    if options.csv is None:
        parser.error('Missing option --csv=.')
    if options.config is None:
        parser.error('Missing option --config=.')
    if options.db_config is None:
        parser.error('Missing option --db-config=.')

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format='%(asctime)s.%(msecs)03d %(name)s: %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    places_csv = absolute_path(options.csv)
    config = config_parser(options.config)
    coll = collections(options.db_config)
    places_coll = coll['places-collection']

    usps_id = config.get('usps', 'user_id')

    delimiter = config.get('csv', 'delimiter')
    delimiter = delimiter.decode('string-escape')
    fieldnames = [
        'id',
        'name',
        'address',
        'address_extended',
        'po_box',
        'locality',
        'region',
        'country',
        'postcode',
        'tel',
        'fax',
        'category',
        'website',
        'email',
        'latitude',
        'longitude',
        'status',
        ]

    log.info('Start...')

    with open(places_csv, 'rb') as places_fp:
        places = csv.DictReader(
            places_fp,
            delimiter=delimiter,
            fieldnames=fieldnames,
            )
        for place in places:
            # Don't store empty fields
            save = defaultdict(dict)
            for k,v in place.iteritems():
                if v != '':
                    save['info'][k] = v

            try:
                lat = float(save['info']['latitude'])
                lng = float(save['info']['longitude'])
            except (KeyError, ValueError):
                log.debug(
                    'Did not find a valid latitude and longitude for place '
                    '{_id}'.format(
                        _id=save['info']['id'],
                        )
                    )
            else:
                save['info']['latitude'] = lat
                save['info']['longitude'] = lng
                # Coordinates are always stored in the form [lng,lat],
                # in that order. Anything else might result in incorrect
                # MongoDB Geospatial queries.
                save['ubernear.location'] = [lng, lat]

                error_msg = ('Bad coordinates (lng,lat) {coord} for id '
                             '{_id}'
                             )
                error_msg = error_msg.format(
                    coord=(lng, lat),
                    _id=save['info']['id']
                    )
                if (lng < -180 or lng >= 180) or (lat < -90 or lat > 90):
                    log.error(error_msg)
                    del save['info']['latitude']
                    del save['info']['longitude']
                    del save['ubernear.location']

            if 'address' not in save['info']:
                log.error(
                    'Found place {_id} with no address information. '
                    'Skipping'.format(
                        _id=save['info']['id'],
                        )
                    )
                continue
            match = dict([
                    ('address', save['info']['address']),
                    ('city', save['info']['locality']),
                    ('state', save['info']['region']),
                    ('zipcode', save['info']['postcode']),
                    ])
            if 'address_extended' in save['info']:
                match['address_extended'] = save['info']['address_extended']
            try:
                norm = address_information.verify(usps_id, match)
            except:
                log.error(
                    'The USPS API could not find an address for place '
                    '{_id}'.format(
                        _id=save['info']['id'],
                        )
                    )
            else:
                norm['name'] = save['info']['name'].upper()
                norm['country'] = 'US'
                save['normalized'] = norm
                save['ubernear.normalization_source'] = 'usps'

            save['ubernear.source'] = 'factual'
            mongo.save_no_replace(
                places_coll,
                _id=save['info']['id'],
                save=save,
                )

    indices = [
        {'ubernear.location': pymongo.GEO2D},
        {'ubernear.last_checked': pymongo.ASCENDING},
        ]
    mongo.create_indices(
        collection=places_coll,
        indices=indices,
        )

    log.info('End')
