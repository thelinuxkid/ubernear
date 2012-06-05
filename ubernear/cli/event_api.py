import optparse
import logging
import pymongo

from paste.gzipper import middleware
from bottle import install, run, default_app
from ubernear.util.config import (
    config_parser,
    collections,
    )
from ubernear.api import EventAPI01, APIServer
from ubernear.util import mongo

log = logging.getLogger(__name__)

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
              'configure google-get'
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
    parser.set_defaults(
        verbose=False,
        )

    options, args = parser.parse_args()
    if args:
        parser.error('Wrong number of arguments.')

    if options.config is None:
        parser.error('Missing option --config=.')
    if options.db_config is None:
        parser.error('Missing option --db-config=.')

    config = config_parser(options.config)
    host = config.get('connection', 'host')
    port = config.get('connection', 'port')

    coll = collections(
        config=options.db_config,
        read_preference=pymongo.ReadPreference.SECONDARY,
        )
    events_coll = coll['events-collection']
    keys_coll = coll['keys-collection']

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format='%(asctime)s.%(msecs)03d %(name)s: %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    indices = [
        {'facebook.start_time': pymongo.ASCENDING},
        ]
    mongo.create_indices(
        collection=events_coll,
        indices=indices,
        )

    uber_api = EventAPI01(
        keys_coll=keys_coll,
        events_coll=events_coll,
        )
    install(uber_api)

    log.info(
        'Starting server http://{host}:{port}'.format(
            host=host,
            port=port,
            )
        )

    app = middleware(default_app())
    run(app=app,
        host=host,
        port=port,
        server=APIServer,
        quiet=True,
        )
