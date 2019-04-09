Installation & Configuration
============================

Quick Install: Running in Docker
--------------------------------

You can run Lakesuperior in Docker for a hands-off quickstart.

`Docker <http://docker.com/>`__ is a containerization platform that
allows you to run services in lightweight virtual machine environments
without having to worry about installing all of the prerequisites on
your host machine.

1. Install the correct `Docker Community
   Edition <https://www.docker.com/community-edition>`__ for your
   operating system.
2. Clone the Lakesuperior git repository:
   ``git clone --recurse-submodules https://github.com/scossu/lakesuperior.git``
3. ``cd`` into repo folder
4. Run ``docker-compose up``

Lakesuperior should now be available at ``http://localhost:8000/``.

The provided Docker configuration includes persistent storage as a
self-container Docker volume, meaning your data will persist between
runs. If you want to clear the decks, simply run
``docker-compose down -v``.

Manual Install (a bit less quick, a bit more power)
---------------------------------------------------

**Note:** These instructions have been tested on Linux. They may work on
Darwin with little modification, and possibly on Windows with some
modifications. Feedback is welcome.

Dependencies
~~~~~~~~~~~~

#. Python 3.6 or greater.
#. A message broker supporting the STOMP protocol. For testing and
   evaluation purposes, `CoilMQ <https://github.com/hozn/coilmq>`__ is
   included with the dependencies and should be automatically installed.

Installation steps
~~~~~~~~~~~~~~~~~~

Start in an empty project folder. If you are feeling lazy you can copy
and paste the lines below in your console.

::

    python3 -m venv venv
    source venv/bin/activate
    pip install lakesuperior
    # Start the message broker. If you have another
    # queue manager listening to port 61613 you can either configure a
    # different port on the application configuration, or use the existing
    # message queue.
    coilmq&
    # Bootstrap the repo
    lsup-admin bootstrap # Confirm manually
    # Run the thing
    fcrepo

Test if it works::

    curl http://localhost:8000/ldp/

Advanced Install
----------------

A "developer mode" install is detailed in the
:ref:`Development Setup<dev_setup>` section.

Configuration
-------------

The app should run for testing and evaluation purposes without any
further configuration. All the application data are stored by default in
the ``data`` directory of the Python package.

This setup is not recommended for anything more than a quick look at the
application. If more complex interaction is needed, or upgrades to the package
are foreseen, it is strongly advised to set up proper locations for
configuration and data.

To change the default configuration you need to:

#. Copy the ``etc.default`` folder to a separate location
#. Set the configuration folder location in the environment:
   ``export FCREPO_CONFIG_DIR=<your config dir location>`` (you can add
   this line at the end of your virtualenv ``activate`` script)
#. Configure the application
#. Bootstrap the app or copy the original data folders to the new
   location if any loction options changed
#. (Re)start the server: ``fcrepo``

The configuration options are documented in the files.

One thing worth noting is that some locations can be specified as relative
paths. These paths will be relative to the ``data_dir`` location specified in
the ``application.yml`` file.

If ``data_dir`` is empty, as it is in the default configuration, it defaults
to the ``data`` directory inside the Python package. This is the option that
one may want to change before anything else.

Production deployment
---------------------

If you like fried repositories for lunch, deploy before 11AM.
