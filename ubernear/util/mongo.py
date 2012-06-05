from collections import OrderedDict

class MongoError(Exception):
    """There was a problem with mongo"""
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return '%s: %s' % (self.__doc__, self._msg)

def _dot_keys(save):
    gen = _dot_keys_recurse(save)
    if gen is not None:
        res = OrderedDict(
            [pair for pair in gen]
            )
        return res

    return save

def _dot_keys_recurse(save):
    if isinstance(save, dict):
        for k,v in save.iteritems():
            if isinstance(v, dict):
                dot = _dot_keys_recurse(v)
                for d in dot:
                    if d is not None:
                        new_key, value = d
                        new_key = '%s.%s' % (k,new_key)
                        yield new_key, value
            else:
                if v is not None:
                    yield k, v
    else:
        yield None

def save_no_replace(
    collection,
    _id,
    save=None,
    add=None,
    add_each=None,
    ):
    changes = OrderedDict()
    if save is not None:
        new_save = _dot_keys(save)
        changes['$set'] = new_save
    if add is not None:
        changes['$addToSet'] = add
    if add_each is not None:
        if '$addToSet' not in changes:
            changes['$addToSet'] = OrderedDict()
        for k,v, in add_each.iteritems():
            changes['$addToSet'][k] = OrderedDict([
                    ('$each', v)
                    ])

    if changes:
        collection.update(
            OrderedDict([
                    ('_id', _id),
                    ]),
            changes,
            upsert=True,
            safe=True,
            )

def create_indices(
    collection,
    indices,
    ):
    for index in indices:
        collection.ensure_index(index.items())
