#!/usr/bin/python
from setuptools import setup, find_packages

EXTRAS_REQUIRES = dict(
    web=[
        'bottle>=0.10.9',
        'paste>=1.7.5.1',
        ],
    mongo=[
        'pymongo>=2.2',
        ],
    util=[
        'python-dateutil>=2.1',
        ],
    facebook=[
        'facepy>=0.6.8',
        ],
    usps=[
        'pyusps>=0.0.1',
        ],
    geo=[
        'pygeocode>=0.0.1',
        ],
    test=[
        'fudge>=1.0.3',
        'nose>=1.1.2',
        ],
    dev=[
        'ipython>=0.12.1',
        ],
    )

# Tests always depend on all other requirements, except dev
for k,v in EXTRAS_REQUIRES.iteritems():
    if k == 'test' or k == 'dev':
        continue
    EXTRAS_REQUIRES['test'] += v

setup(
    name='ubernear',
    version='0.0.1',
    description=(
        "ubernear -- Ubernear monitors Facebook event owners for new "
        " events and allows geo-spatial searches on events via a "
        " RESTful API"
        ),
    packages = find_packages(),
    namespace_packages = ['ubernear'],
    test_suite='nose.collector',
    install_requires=[
        'setuptools',
        ],
    extras_require=EXTRAS_REQUIRES,
    entry_points={
        'console_scripts': [
            'facebook-owner = ubernear.cli.facebook_owner:main [mongo,facebook,util]',
            'facebook-event = ubernear.cli.facebook_event:main [mongo,facebook,usps]',
            'factual-import = ubernear.cli.factual_import:main [mongo]',
            'place-geocode = ubernear.cli.place_geocode:main [mongo,util,geo]',
            'event-location = ubernear.cli.event_location:main [mongo,util]',
            'event-api = ubernear.cli.event_api:main [web,mongo,util]',
            ],
        },
    )
