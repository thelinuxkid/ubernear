import optparse
import logging
import pymongo
import time
import random

from facepy import GraphAPI

from ubernear.util import signal_handler
from ubernear import facebook_event
from ubernear.util.config import (
    collections,
    config_parser,
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
        '--config',
        help=('Path to the file with information on how to '
              'configure facebook-event'
              ),
        metavar='PATH',
        )
    parser.add_option(
        '--db-config',
        help=('Path to the file with information on how to '
              'retrieve and store data in the database'
              ),
        metavar='PATH',
        )
    parser.add_option(
        '-a', '--process-all',
        help=('Process all events that have not expired '
              'instead of just those that have not been '
              'processed [default %default]'
              ),
        action="store_true", dest="process_all"
        )
    parser.set_defaults(
        verbose=False,
        process_all=False,
        )

    options, args = parser.parse_args()
    if args:
        parser.error('Wrong number of arguments.')

    if options.config is None:
        parser.error('Missing option --config=.')
    if options.db_config is None:
        parser.error('Missing option --db-config=.')

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format='%(asctime)s.%(msecs)03d %(name)s: %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    config = config_parser(options.config)
    access_token = config.get('facebook', 'access_token')
    graph = GraphAPI(access_token)

    usps_id = config.get('usps', 'user_id')
    yahoo_id = config.get('yahoo', 'app_id')

    coll = collections(options.db_config)
    events_coll = coll['events-collection']
    expired_coll = coll['expired-collection']

    indices = [
        {'facebook.end_time': pymongo.ASCENDING},
        {'ubernear.fetched': pymongo.ASCENDING},
        ]
    mongo.create_indices(
        collection=events_coll,
        indices=indices,
        )
    indices = [
        {'facebook.end_time': pymongo.ASCENDING},
        ]
    mongo.create_indices(
        collection=expired_coll,
        indices=indices,
        )

    log.info('Start...')

    log.info('Moving expired events...')
    facebook_event.expire(
        events_coll=events_coll,
        expired_coll=expired_coll,
        )

    log.info('Updating event data...')
    facebook_work = facebook_event.update_facebook(
        events_coll=events_coll,
        graph=graph,
        process_all=options.process_all,
        )

    log.info('Updating venue data...')
    venue_work = facebook_event.update_venue(
        events_coll=events_coll,
        usps_id=usps_id,
        process_all=options.process_all,
        )

    log.info('Updating coordinate data...')
    coord_work = facebook_event.update_coordinate(
        events_coll=events_coll,
        yahoo_id=yahoo_id,
        process_all=options.process_all,
        )
    if coord_work['sleep'] is not None:
        delay = coord_work['sleep']
        log.info(
            'Geocoding rate limit reached. '
            'Sleeping {sleep} hours...'.format(
                delay=delay,
                )
            )
        time.sleep(delay)
    else:
        found_work = (
            facebook_work
            or
            venue_work
            or
            coord_work['found_work']
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
