import logging

import stomp


class StompHandler(logging.Handler):
    '''
    Send messages to a remote queue broker using the STOMP protocol.

    This module is named and configured separately from
    standard logging for clarity about its scope: while logging has an
    informational purpose, this module has a functional one.
    '''
    def __init__(self, conf):
        self.conf = conf
        if self.conf['protocol'] == '11':
            conn_cls = stomp.Connection12
        elif self.conf['protocol'] == '12':
            conn_cls = stomp.Connection11
        else:
            conn_cls = stomp.Connection10

        self.conn = conn_cls([(self.conf['host'], self.conf['port'])])
        self.conn.start()
        self.conn.connect(
            username=self.conf['username'],
            passcode=self.conf['password'],
            wait=True
        )

        return super().__init__()


    def __del_(self):
        '''
        Disconnect the client.
        '''
        self.conn.disconnect()

    def emit(self, record):
        '''
        Send the message to the destination endpoint.
        '''
        self.conn.send(destination=self.conf['destination'],
                body=bytes(self.format(record), 'utf-8'))

