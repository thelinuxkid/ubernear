import fudge

from nose.tools import eq_ as eq
from collections import OrderedDict

from ubernear.util import mongo

def test_save_no_replace_simple():
    collection = fudge.Fake('collection')
    collection.remember_order()

    update = collection.expects('update')
    fields = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    update.with_args(
        OrderedDict([
                ('_id', 'foo_id'),
                ]),
        OrderedDict([
                ('$set', fields),
                ]),
        upsert=True,
        safe=True,
        )

    save = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    mongo.save_no_replace(
        collection,
        'foo_id',
        save=save,
        )

def test_save_no_replace_save_none():
    collection = fudge.Fake('collection')
    collection.remember_order()

    update = collection.expects('update')
    fields2 = OrderedDict([
            ('fee', 'fi'),
            ('fo', 'fum'),
            ])
    update.with_args(
        OrderedDict([
                ('_id', 'foo_id'),
                ]),
        OrderedDict([
                ('$addToSet', fields2),
                ]),
        upsert=True,
        safe=True,
        )

    add = OrderedDict([
            ('fee', 'fi'),
            ('fo', 'fum'),
            ])
    mongo.save_no_replace(
        collection,
        'foo_id',
        add=add,
        )

def test_save_no_replace_add():
    collection = fudge.Fake('collection')
    collection.remember_order()

    update = collection.expects('update')
    fields1 = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    fields2 = OrderedDict([
            ('fee', 'fi'),
            ('fo', 'fum'),
            ])
    update.with_args(
        OrderedDict([
                ('_id', 'foo_id'),
                ]),
        OrderedDict([
                ('$set', fields1),
                ('$addToSet', fields2),
                ]),
        upsert=True,
        safe=True,
        )

    save = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    add = OrderedDict([
            ('fee', 'fi'),
            ('fo', 'fum'),
            ])
    mongo.save_no_replace(
        collection,
        'foo_id',
        save=save,
        add=add,
        )

def test_save_no_replace_add_and_add_each():
    collection = fudge.Fake('collection')
    collection.remember_order()

    update = collection.expects('update')
    fields1 = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    fields2 = OrderedDict([
            ('fi', 'fo'),
            ('fo', 'fum'),
            ('fee', OrderedDict([
                        ('$each', ['fo', 'fi', 'fum']),
                        ]),
             ),
            ('fum', OrderedDict([
                        ('$each', ['fi', 'fo', 'fee']),
                        ]),
             ),
            ])
    update.with_args(
        OrderedDict([
                ('_id', 'foo_id'),
                ]),
        OrderedDict([
                ('$set', fields1),
                ('$addToSet', fields2),
                ]),
        upsert=True,
        safe=True,
        )

    save = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    add = OrderedDict([
            ('fi', 'fo'),
            ('fo', 'fum'),
            ])
    add_each = OrderedDict([
            ('fee', ['fo', 'fi', 'fum']),
            ('fum', ['fi', 'fo', 'fee']),
            ])
    mongo.save_no_replace(
        collection,
        'foo_id',
        save=save,
        add=add,
        add_each=add_each,
        )

def test_save_no_replace_add_each():
    collection = fudge.Fake('collection')
    collection.remember_order()

    update = collection.expects('update')
    fields1 = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    fields2 = OrderedDict([
            ('fee', OrderedDict([
                        ('$each', ['fo', 'fi', 'fum']),
                        ]),
             ),
            ('fum', OrderedDict([
                        ('$each', ['fi', 'fo', 'fee']),
                        ]),
             ),
            ])
    update.with_args(
        OrderedDict([
                ('_id', 'foo_id'),
                ]),
        OrderedDict([
                ('$set', fields1),
                ('$addToSet', fields2),
                ]),
        upsert=True,
        safe=True,
        )

    save = OrderedDict([
            ('foo', 'bar'),
            ('sna', 'foo'),
            ])
    add_each = OrderedDict([
            ('fee', ['fo', 'fi', 'fum']),
            ('fum', ['fi', 'fo', 'fee']),
            ])
    mongo.save_no_replace(
        collection,
        'foo_id',
        save=save,
        add_each=add_each,
        )

def test_save_no_replace_noop():
    collection = fudge.Fake('collection')
    mongo.save_no_replace(
        collection,
        'foo_id',
        )

def test_create_indices_simple():
    collection = fudge.Fake('collection')
    collection.remember_order()

    ensure_index = collection.expects('ensure_index')
    ensure_index.with_args([
            ('foo', 'bar'),
            ])

    ensure_index = collection.next_call('ensure_index')
    ensure_index.with_args([
            ('sna', 'foo'),
            ('fee', 'fi'),
            ])

    indices = [
        OrderedDict([
                ('foo', 'bar')
                ]),
        OrderedDict([
                ('sna', 'foo'),
                ('fee', 'fi')
                ]),
        ]
    mongo.create_indices(
        collection=collection,
        indices=indices,
        )

def test_dot_keys_simple():
    save = OrderedDict([
            ('a', OrderedDict([
                        ('b', 'foo'),
                        ('c', 'bar'),
                        ]),
             ),
            ('e', 'sna'),
            ])

    save = mongo._dot_keys(save)

    expected = OrderedDict([
            ('a.b', 'foo'),
            ('a.c', 'bar'),
            ('e', 'sna'),
            ])
    eq(save, expected)

def test_dot_keys_multiple():
    save = OrderedDict([
            ('a', OrderedDict([
                        ('b', OrderedDict([
                                    ('c', OrderedDict([
                                                ('e', 'foo'),
                                                ('f', 'bar'),
                                                ]),
                                     ),
                                    ('g', 'sna'),
                                    ]),
                         ),
                        ('h', 'fi'),
                        ]),
             ),
             ('i', 'foo'),
            ])

    save = mongo._dot_keys(save)

    expected = OrderedDict([
            ('a.b.c.e', 'foo'),
            ('a.b.c.f', 'bar'),
            ('a.b.g', 'sna'),
            ('a.h', 'fi'),
            ('i', 'foo'),
            ])
    eq(save, expected)

def test_dot_keys_already_dotted():
    save = OrderedDict([
            ('a.x', OrderedDict([
                        ('b', 'foo'),
                        ('c.y', 'bar'),
                        ]),
             ),
            ('e', 'sna'),
            ])

    save = mongo._dot_keys(save)

    expected = OrderedDict([
            ('a.x.b', 'foo'),
            ('a.x.c.y', 'bar'),
            ('e', 'sna'),
            ])
    eq(save, expected)

def test_dot_keys_none():
    save = OrderedDict([
            ('a', OrderedDict([
                        ('b', OrderedDict([
                                    ('c', OrderedDict([
                                                ('e', None),
                                                ('f', 'bar'),
                                                ]),
                                     ),
                                    ('g', 'sna'),
                                    ]),
                         ),
                        ('h', None),
                        ]),
             ),
             ('i', None),
            ])

    save = mongo._dot_keys(save)

    expected = OrderedDict([
            ('a.b.c.f', 'bar'),
            ('a.b.g', 'sna'),
            ])
    eq(save, expected)
