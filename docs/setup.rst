Installation & Configuration
============================

Quick Install: Running in Docker
--------------------------------

You can run LAKEsuperior in Docker for a hands-off quickstart.

`Docker <http://docker.com/>`__ is a containerization platform that
allows you to run services in lightweight virtual machine environments
without having to worry about installing all of the prerequisites on
your host machine.

1. Install the correct `Docker Community
   Edition <https://www.docker.com/community-edition>`__ for your
   operating system.
2. Clone the LAKEsuperior git repository:
   ``git clone https://github.com/scossu/lakesuperior.git``
3. ``cd`` into repo folder
4. Run ``docker-compose up``

LAKEsuperior should now be available at ``http://localhost:8000/``.

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

1. Python 3.5 or greater.
2. A message broker supporting the STOMP protocol. For testing and
   evaluation purposes, `CoilMQ <https://github.com/hozn/coilmq>`__ is
   included with the dependencies and should be automatically installed.

Installation steps
~~~~~~~~~~~~~~~~~~

1. Create a virtualenv in a project folder:
   ``virtualenv -p <python 3.5+ exec path> <virtualenv folder>``
2. Activate the virtualenv: ``source <path_to_virtualenv>/bin/activate``
3. Clone this repo:
   ``git clone https://github.com/scossu/lakesuperior.git``
4. ``cd`` into repo folder
5. Install dependencies: ``pip install -r requirements.txt``
6. Start your STOMP broker, e.g.: ``coilmq &``. If you have another
   queue manager listening to port 61613 you can either configure a
   different port on the application configuration, or use the existing
   message queue.
7. Run ``./lsup-admin bootstrap`` to initialize the binary and graph
   stores
8. Run ``./fcrepo``.

Configuration
-------------

The app should run for testing and evaluation purposes without any
further configuration. All the application data are stored by default in
the ``data`` directory.

To change the default configuration you should:

1. Copy the ``etc.skeleton`` folder to a separate location
2. Set the configuration folder location in the environment:
   ``export FCREPO_CONFIG_DIR=<your config dir location>`` (you can add
   this line at the end of your virtualenv ``activate`` script)
3. Configure the application
4. Bootstrap the app or copy the original data folders to the new
   location if any loction options changed
5. (Re)start the server: ``./fcrepo``

The configuration options are documented in the files.

**Note:** ``test.yml`` must specify a different location for the graph
and for the binary stores than the default one, otherwise running a test
suite will destroy your main data store. The application will issue an
error message and refuse to start if these locations overlap.

Production deployment
---------------------

If you like fried repositories for lunch, deploy before 11AM.
