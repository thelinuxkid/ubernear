import optparse
import logging
import pymongo
import time
import random

from ubernear.util import signal_handler
from ubernear import event_location
from ubernear.util.config import (
    collections,
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

    if options.db_config is None:
        parser.error('Missing option --db-config=.')

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format='%(asctime)s.%(msecs)03d %(name)s: %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    coll = collections(options.db_config)
    events_coll = coll['events-collection']
    places_coll = coll['places-collection']
    database = coll['database']

    indices = [
        {'match.ubernear.location': pymongo.GEO2D},
        ]
    mongo.create_indices(
        collection=events_coll,
        indices=indices,
        )

    log.info('Start...')

    log.info('Setting events\' locations...')
    found_work = event_location.locate(
        events_coll=events_coll,
        places_coll=places_coll,
        database=database,
        process_all=options.process_all,
        )

    if not found_work:
        minutes = 15
        delay = random.randint(60*minutes-1, 60*minutes+1)
        log.info(
            'Did not find any work. '
            'Sleeping about {minutes} minutes...'.format(
                minutes=minutes,
                )
            )
        time.sleep(delay)

    log.info('End')
