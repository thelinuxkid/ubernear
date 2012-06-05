import re
import optparse
import logging
import time
import random
import pymongo

from collections import OrderedDict
from facepy import GraphAPI
from facepy.exceptions import FacepyError
from datetime import datetime, timedelta

from ubernear.util import signal_handler
from ubernear.util import (
    utc_from_iso8601,
    )
from ubernear.util import mongo
from ubernear.util.config import (
    collections,
    config_parser,
    )

log = logging.getLogger(__name__)

batch_size = 50

def _mark_as_failed(
    owners_coll,
    owner_id,
    now,
    reason='',
    ):
    save = OrderedDict([
            ('ubernear', OrderedDict([
                        ('lookup_failed', OrderedDict([
                                    # If owner is retried and it is
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
        owners_coll,
        _id=owner_id,
        save=save,
        )

def _save_events(
    events_coll,
    expired_coll,
    owners_coll,
    owner_ids,
    graph
    ):
    # Don't waste a call to the Facebook Graph API
    if not owner_ids:
        return

    batch = [
        OrderedDict([
                ('method', 'GET'),
                ('relative_url',
                 '{owner_id}/events?date_format=c'.format(
                     owner_id=owner_id,
                     ),
                 ),
                ])
        for owner_id in owner_ids
        ]
    reponses = graph.batch(batch)

    now = datetime.utcnow()
    for owner_id,response in zip(owner_ids,reponses):
        if isinstance(response, FacepyError):
            _mark_as_failed(
                owners_coll=owners_coll,
                owner_id=owner_id,
                now=now,
                reason=str(response),
                )
            continue
        # Object does not exist anymore
        if response is False:
            _mark_as_failed(
                owners_coll=owners_coll,
                owner_id=owner_id,
                now=now,
                reason='False response',
                )
            continue
        if response is None:
            # None has special significance in mongodb searches
            # so use 'null' instead.
            _mark_as_failed(
                owners_coll=owners_coll,
                owner_id=owner_id,
                now=now,
                reason='Null response',
                )
            continue

        for event in response['data']:
            _id = event.pop('id')
            # TODO find a more efficient way to do this
            if (events_coll.find_one({'_id': _id})
                or
                expired_coll.find_one({'_id': _id})
                ):
                continue
            try:
                # This information is not complete. Save it for now
                # but allow it to be replaced with more detailed
                # information later.
                # TODO what happens if this overwrites good values?
                # Is that OK? Is it bad?
                # The check above does not guarantee some other
                # process won't update this same event before we get
                # here
                save = OrderedDict([
                        ('facebook', event),
                        ('ubernear', OrderedDict([
                                    ('source', 'facebook'),
                                    ('fetched', now),
                                    ]),
                         ),
                        ])
                save['facebook']['start_time'] = utc_from_iso8601(
                    save['facebook']['start_time'],
                    naive=True,
                    )
                save['facebook']['end_time'] = utc_from_iso8601(
                    save['facebook']['end_time'],
                    naive=True,
                    )
                if 'updated_time' in save['facebook']:
                    save['facebook']['updated_time'] = utc_from_iso8601(
                        save['facebook']['updated_time'],
                        naive=True,
                        )
                mongo.save_no_replace(
                    events_coll,
                    _id=_id,
                    save=save,
                    )
            except Exception:
                log.error(
                    'Could not save event {_id}'.format(
                        _id=_id,
                        )
                    )

        try:
            save = {'ubernear.last_lookup': now}
            mongo.save_no_replace(
                owners_coll,
                _id=owner_id,
                save=save,
                )
        except Exception:
            log.error(
                'Could not update owner {_id}'.format(
                    _id=owner_id,
                    )
                )

def update_owners(
    events_coll,
    expired_coll,
    owners_coll,
    ):
    # Insert any owner ids that have migrated and mark the current
    # owners with ubernear.id_migrated = True
    find_regex = '\(#21\) Page ID [0-9]+ was migrated to page ID [0-9]+'
    cursor = owners_coll.find(
        {'$and':[
                {'ubernear.lookup_failed.reason': {'$regex': find_regex}},
                {'ubernear.id_migrated': {'$ne': True}},
         ],
        }
        )
    regex = ('\(#21\) Page ID (?P<old_id>[0-9]+) was migrated to '
             'page ID (?P<new_id>[0-9]+)'
             )
    for owner in cursor:
        reason = owner['ubernear']['lookup_failed']['reason']
        match = re.search(regex, reason)
        if match is None:
            log.error(
                'Found unexpected lookup_failed reason {reason} '
                'for owner {_id}. Skipping'.format(
                    reason=reason,
                    _id=owner['_id'],
                    )
                )
        old_id = match.group('old_id')
        new_id = match.group('new_id')
        if old_id != owner['_id']:
            log.warn(
                'lookup_failed reason {reason} for owner {_id} has a '
                'different id {old_id}.'.format(
                    old_id=old_id,
                    new_id=new_id,
                    _id=owner['_id'],
                    )
                )
        # TODO find a more efficient way to do this
        one = owners_coll.find_one({'_id': new_id})
        if one is None:
            owners_coll.insert({'_id': new_id})
        save = OrderedDict([
                ('ubernear.id_migrated', True)
                ])
        mongo.save_no_replace(
            owners_coll,
            _id=owner['_id'],
            save=save,
            )

def update_owner_events(
    events_coll,
    expired_coll,
    owners_coll,
    graph,
    ):
    # TODO filter out owners that we don't care about, e.g.,
    # owners of events not in the geographical areas we are
    # interested in.
    refresh_days = 1
    last_lookup = datetime.utcnow() - timedelta(days=refresh_days)
    owner_batch = []
    found_work = False
    or_query = {
        '$or': [
            {'ubernear.last_lookup': {'$exists': False}},
            {'ubernear.last_lookup': {'$lt': last_lookup}},
            ],
        }
    migrated_query = {'ubernear.id_migrated': {'$ne': True}}
    cursor = owners_coll.find(
        {'$and': [
                or_query,
                migrated_query,
                ],
         },
        sort=[('ubernear.last_lookup', pymongo.ASCENDING)],
        )
    for owner in cursor:
        found_work = True
        owner_batch.append(owner['_id'])
        if len(owner_batch) == batch_size:
            _save_events(
                events_coll=events_coll,
                expired_coll=expired_coll,
                owners_coll=owners_coll,
                owner_ids=owner_batch,
                graph=graph,
                )
            owner_batch = []

    _save_events(
        events_coll=events_coll,
        expired_coll=expired_coll,
        owners_coll=owners_coll,
        owner_ids=owner_batch,
        graph=graph,
        )

    return found_work

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
              'configure facebook-put'
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

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format='%(asctime)s.%(msecs)03d %(name)s: %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    config = config_parser(options.config)
    access_token = config.get('facebook', 'access_token')
    graph = GraphAPI(access_token)

    coll = collections(options.db_config)
    events_coll = coll['events-collection']
    expired_coll = coll['expired-collection']
    owners_coll = coll['owners-collection']

    indices = [
        {'ubernear.last_lookup': pymongo.ASCENDING},
        ]
    mongo.create_indices(
        collection=owners_coll,
        indices=indices,
        )

    log.info('Start...')

    log.info('Updating owners...')
    update_owners(
        events_coll=events_coll,
        expired_coll=expired_coll,
        owners_coll=owners_coll,
        )

    log.info('Updating owners\' events...')
    found_work = update_owner_events(
        events_coll=events_coll,
        expired_coll=expired_coll,
        owners_coll=owners_coll,
        graph=graph,
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
