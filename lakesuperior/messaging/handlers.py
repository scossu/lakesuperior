import logging

import stomp


logger = logging.getLogger(__name__)


class StompHandler(logging.Handler):
    """
    Send messages to a remote queue broker using the STOMP protocol.

    This module is named and configured separately from
    standard logging for clarity about its scope: while logging has an
    informational purpose, this module has a functional one.
    """
    def __init__(self, conf):
        self.conf = conf
        if self.conf['protocol'] == '11':
            conn_cls = stomp.Connection11
        elif self.conf['protocol'] == '12':
            conn_cls = stomp.Connection12
        else:
            conn_cls = stomp.Connection10

        self.conn = conn_cls([(self.conf['host'], self.conf['port'])])
        self.conn.start()
        try:
            self.conn.connect(
                username=self.conf['username'],
                passcode=self.conf['password'],
                wait=True
            )
        except stomp.exception.ConnectFailedException:
            logger.warning(
                    'Could not connect to the STOMP server. Your messages '
                    'will be ditched.')

        return super().__init__()


    def __del_(self):
        """Disconnect the client."""
        if self.conn.is_connected():
            self.conn.disconnect()

    def emit(self, record):
        """Send the message to the destination endpoint."""
        if self.conn.is_connected():
            self.conn.send(destination=self.conf['destination'],
                    body=bytes(self.format(record), 'utf-8'))
        else:
            logger.warning('STOMP server not connected. Message dropped.')

