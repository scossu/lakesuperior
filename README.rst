LAKEsuperior
============

|build status| |docs|

LAKEsuperior is an alternative `Fedora
Repository <http://fedorarepository.org>`__ implementation.

Documentation
-------------

The full documentation is maintained in `Read The Docs
<http://lakesuperior.readthedocs.io/>`__. Please refer to that for more info.

Installation
------------

The following instructions are aimed at a manual install using this git
repository. For a hands-off install using Docker, see
`the setup documentation
<http://lakesuperior.readthedocs.io/en/latest/setup.html>`__.

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

Contributing
------------

This has been so far a single person’s off-hours project (with much
input from several sides). In order to turn into anything close to a
Beta release and eventually to a production-ready implementation, it
needs some community love.

Contributions are welcome in all forms, including ideas, issue reports,
or even just spinning up the software and providing some feedback.
LAKEsuperior is meant to live as a community project.

See `related document
<http://lakesuperior.readthedocs.io/en/development/contributing.html>`__
for further details onhow to fork, improve, document and test the project.

.. |build status| image:: http://img.shields.io/travis/scossu/lakesuperior/master.svg?style=flat
   :alt: Build Status
   :target: https://travis-ci.org/username/repo

.. |docs| image:: https://readthedocs.org/projects/lakesuperior/badge/
    :alt: Documentation Status
    :scale: 100%
    :target: https://lakesuperior.readthedocs.io/en/latest/?badge=latest
