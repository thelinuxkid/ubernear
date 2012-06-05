import os
import pymongo

from ConfigParser import SafeConfigParser

def absolute_path(path):
    path = os.path.expanduser(path)
    path = os.path.abspath(path)

    return path

def config_parser(path):
    """
    Load a config file
    """
    path = absolute_path(path)
    config = SafeConfigParser()
    with open(path) as fp:
        config.readfp(fp)

    return config

def _db_config_parts(
    path,
    host='localhost:27017',
    ):
    config = config_parser(path)

    connection = dict(config.items('connection'))
    if 'host' not in connection:
        connection['host'] =  host

    collections = dict(config.items('collection'))

    return connection, collections

def collections(
    config,
    read_preference=None,
    ):
    connection, collections = _db_config_parts(config)
    host = connection['host']
    replica_set = connection.get('replica-set')
    database = connection['database']

    if replica_set:
        connection = pymongo.ReplicaSetConnection(
            host,
            replicaSet=replica_set,
            )
        # ReadPreference.PRIMARY is the default
        if read_preference is not None:
            connection.read_preference = read_preference
    else:
        connection = pymongo.Connection(host)

    database = connection[database]
    collections = dict(
        [(k,database[v])
         for k,v
         in collections.items()
         ]
        )
    collections['database'] = database

    return collections
