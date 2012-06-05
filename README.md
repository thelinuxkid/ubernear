========
Ubernear
========

Ubernear was originally the name of a startup which aimed to aggregate
and normalize Facebook events, allowing users to discover what was
going on around them in the next few hours. The Ubernear Open Source
Project is a slimmed down, more general take on the startup's
version. It automates event collection by monitoring event owners and
does its best to fill in incomplete venue data, such as
coordinates. The events can be accessed via a RESTful API which
supports geo-spatial queries and other filters, such as time spans.

Event Collection Jobs
=====================

It is recommended that all background jobs be run as service daemons.
To start collecting data you must have a MongoDB collection with event
owner ids.

facebook-owner
--------------
The main function of the facebook-owner job is to fetch events for the
owners ids stored in the database. You can run facebook-owner like
this::

    ./facebook-owner --config=facebook-owner.cfg --db-config=mongodb.cfg

Where facebook-owner.cfg looks like::

    [facebook]
    access_token = <facebook-access-token>

And mongodb.cfg looks like::

    [connection]
    host = <hostname>:<port>
    replica-set = <replicaset-name>
    database = <database-name>

    [collection]
    places-collection = <collection-name>
    proxies-collection = <collection-name>
    events-collection = <collection-name>
    expired-collection = <collection-name>
    owners-collection = <collection-name>
    keys-collection = <collection-name>

The replica-set option is not necessary. If you are not using a replica
set in your MongoDB setup do not include this line.
All jobs take in the database configuration as a separate command line
parameter so that the same configuration can be used for all jobs.

facebook-event
--------------
The main function of the facebook-event job is to fill in the details
for each event, even when Facebook does not return all the details. To
fill in missing details Ubernear uses the USPS Address Information API
to normalize addresses and the Yahoo PlaceFinder API to perform
geocoding on the venues without coordinates. You can run
facebook-event like this::

    ./facebook-event --config=facebook-event.cfg --db-config=mongodb.cfg

Where facebook-event.cfg looks like::

    [facebook]
    access_token = <facebook-access-token>

    [usps]
    user_id = <usps-user-id>

    [yahoo]
    app_id = <yahoo-app-id>

And mongodb.cfg is the same as facebook-owner's.

event-location
--------------
This job tries to match an event venue with a place in the places
MongoDB collection. If no match is found it fills in the field where
the match would normally go with the venue information from Facebook,
provided the venue information has some required fields set. You can
run event-location like this::

    ./event-location --db-config=mongodb.cfg

Where mongodb.cfg is the same as facebook-owner's.

API
===

Ubernear comes with a RESTful API which allows events to be served by
their coordinates. It also allows simple time filtering. The API
restricts access by requiring an API key. Access is granted or denied
based on this key and the location of the request. You can run the API
like this::

    ./event-api --config=event-api.cfg --db-config=mongodb.cfg

Where event-api.cfg looks like::

      [connection]
      host = <hostname>
      port = <port>

And mongodb.cfg is the same as facebook-owner's.

Building
========

External dependencies
---------------------

    - libxml2-dev
    - libxslt1-dev
    - build-essential
    - python-dev
    - python-setuptools
    - python-virtualenv

Developing
----------

To start developing run the following commands from the project's
base directory::

    # I like to install the virtual environment in its own
    # hidden repo but you don't have to
    virtualenv --no-site-packages .virtual
    # I leave the magic to Ruby developers (.virtual/bin/activate)
    # but you don't have to agree with me
    .virtual/bin/python setup.py develop
    # Install development and testing dependecies. Pip doesn't seem
    # to handle extras_require yet: https://github.com/pypa/pip/issues/7.
    # So, use easy_install
    # At this point, uberget will already be in easy-install.pth.
    # So easy_install will not attempt to download it
    .virtual/bin/easy_install ubernear.uberget[dev,test]

Testing
-------

To run the unit-tests run the following command from the project's
base directory::

    .virtual/bin/nosetests

Installing
----------

To install the project run the following commands from the project's
base directory::

    # I like to install the virtual environment in its own
    # hidden repo but you don't have to
    virtualenv --no-site-packages .virtual
    # I leave the magic to Ruby developers (.virtual/bin/activate)
    # but you don't have to agree with me
    .virtual/bin/python setup.py install
    # Install development and testing dependecies. Pip doesn't seem
    # to handle extras_require yet: https://github.com/pypa/pip/issues/7.
    # So, use easy_install
    # At this point, uberget will already be in easy-install.pth.
    # So easy_install will not attempt to download it

    # Depending on the cli you wish to run install the appropriate
    # dependencies as listed in setup.py. For example to run
    # factual-import you would install the graph dependency
    .virtual/bin/easy_install ubernear.uberget[mongo]

    # If you wish to install all the dependencies and run the
    # tests run
    .virtual/bin/easy_install ubernear.uberget[test]
