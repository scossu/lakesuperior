LAKEsuperior Messaging
======================

LAKEsuperior implements a messaging system based on ActivityStreams, as
indicated by the `Fedora API
specs <https://fedora.info/2017/06/30/spec/#notifications>`__. The
metadata set provided is currently quite minimal but can be easily
enriched by extending the
:class:`~lakesuperior.messaging.messenger.Messenger` class.

STOMP is the only supported protocol at the moment. More protocols may
be made available at a later time.

LAKEsuperior can send messages to any number of destinations: see
:doc:`setup`.

By default, `CoilMQ <https://github.com/hozn/coilmq>`__ is provided for testing
purposes and listens to ``localhost:61613``. The default route sends messages
to ``/topic/fcrepo``.

A small command-line utility, also provided with the Python
dependencies, allows to watch incoming messages. To monitor messages,
enter the following *after activating your virtualenv*:

::

    stomp -H localhost -P 61613 -L /topic/fcrepo

See the `stomp.py library reference
page <https://github.com/jasonrbriggs/stomp.py/wiki/Command-Line-Access>`__
for details.

Disabing messaging
------------------

Messaging is enabled by default in LAKEsuperior. If you are not interested in
interacting with an integration framework, you can save yourself some I/O and
complexity and turn messaging off completely. In order to do that, set all
entries in the ``routes`` section of ``application.yml`` to not active, e.g.::

    [...]
    messaging:
        routes:
        - handler: StompHandler
          active: False # ← Disable the route
              protocol: '11'
              host: 127.0.0.1
              port: 61613
              username:
              password:
              destination: '/topic/fcrepo'
              formatter: ASResourceFormatter

A message queue does not need to be running in order for LAKEsuperior to
operate, even if one or more routes are active. In that case, the application
will throw a few ugly mssages and move on. *TODO: This should be handled more
gracefully.*
