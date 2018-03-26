# LAKEsuperior Messaging

LAKEsuperior implements a messaging system based on ActivityStreams, as
indicated by the
[Feodra API specs](https://fedora.info/2017/06/30/spec/#notifications).
The metadata set provided is currently quite minimal but can be easily
enriched by extending the
[default formatter class](https://github.com/scossu/lakesuperior/blob/master/lakesuperior/messaging/messenger.py).

STOMP is the only supported protocol at the moment. More protocols may be made
available at a later time.

LAKEsuperior can send messages to any number of destinations: see
[configuration](https://github.com/scossu/lakesuperior/blob/master/etc.defaults/application.yml#L79).
By default, CoilMQ is provided for testing purposes and listens to
`localhost:61613`. The default route sends messages to `/topic/fcrepo`.

A small command-line utility, also provided with the Python dependencies,
allows to watch incoming messages. To monitor messages, enter the following
*after activating your virtualenv*:

```
stomp -H localhost -P 61613 -L /topic/fcrepo
```

See the [stomp.py library reference page](https://github.com/jasonrbriggs/stomp.py/wiki/Command-Line-Access)
for details.
