import optparse
import logging
import time
import random

from contextlib import contextmanager
from pygeocode import geocoder

from ubernear.util import signal_handler
from ubernear.util.config import (
    collections,
    config_parser,
    )
from ubernear.util import mongo

log = logging.getLogger(__name__)

@contextmanager
def _places_cursor(places_coll):
    query = {'ubernear.location': {'$exists': False}}
    cursor = places_coll.find(
        spec=query,
        timeout=False,
        )

    try:
        yield cursor
    except Exception, e:
        raise e
    finally:
        log.info('Closing cursor connection')
        del cursor

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
        '--db-config',
        help=('Path to the file with information on how to '
              'retrieve and store data in the database'
              ),
        metavar='PATH',
        )
    parser.add_option(
        '--config',
        help=('Path to the file with information on application '
              'ids for different services'
              ),
        metavar='PATH',
        )
    parser.set_defaults(
        verbose=False,
        )

    options, args = parser.parse_args()
    if args:
        parser.error('Wrong number of arguments.')

    if options.db_config is None:
        parser.error('Missing option --db-config=.')

    if options.config is None:
        parser.error('Missing option --config=.')

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format='%(asctime)s.%(msecs)03d %(name)s: %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    log.info('Start...')

    config = config_parser(options.config)
    yahoo_appid = config.get('yahoo','appid')

    coll = collections(options.db_config)
    places_coll = coll['places-collection']

    found_work = False
    with _places_cursor(places_coll=places_coll) as cursor:
        for place in cursor:
            found_work = True
            address_tmpl = (
                '{address} {extended}, {locality}, {region} '
                '{postcode}, {country}'
                )
            info = place['info']
            address = address_tmpl.format(
                address=info.get('address', ''),
                extended=info.get('address_extended', ''),
                locality=info.get('locality', ''),
                region=info.get('region', ''),
                postcode=info.get('postcode', ''),
                country=info.get('country', ''),
                )
            log.debug(
                'Geocoding {address}'.format(
                    address=address,
                    )
                )
            try:
                location = geocoder.geocode_yahoo(
                    address=address,
                    yahoo_appid=yahoo_appid,
                    )
            except geocoder.GeocoderRateLimitError, e:
                log.info(
                    '{msg}. Sleeping 24 hours...'.format(
                        msg=str(e),
                        )
                    )
                time.sleep(60*60*24)
            except geocoder.GeocoderError, e:
                log.error(
                    '{msg}. Skipping place {_id}'.format(
                        msg=str(e),
                        _id=place['_id'],
                        )
                    )
            else:
                # Coordinates are always stored in the form [lng,lat],
                # in that order. Anything else might result in incorrect
                # MongoDB Geospatial queries.
                lat = float(location['lat'])
                lng = float(location['lng'])
                location = [lng, lat]
                save = {
                    'ubernear.location': location,
                    'info.latitude': lat,
                    'info.longitude': lng,
                    }
                mongo.save_no_replace(
                    places_coll,
                    _id=place['_id'],
                    save=save,
                    )

    if not found_work:
        hours = 24
        delay = random.randint(60*60*hours, 60*60*hours+1)
        log.info(
            'Did not find any work. '
            'Sleeping about {hours} hours...'.format(
                hours=hours,
                )
            )
        time.sleep(delay)

    log.info('End')
