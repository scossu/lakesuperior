import logging

from abc import ABCMeta, abstractmethod

import stomp

from flask import current_app


class StompHandler(logging.Handler):
    '''
    Send messages to a remote queue broker using the STOMP protocol.

    This module is named and configured separately from
    standard logging for clarity about its scope: while logging has an
    informational purpose, this module has a functional one.
    '''
    def __init__(self, conf):
        self.conf = conf
        self.conn = stomp.Connection([(conf['host'], conf['port'])])
        self.conn.start()
        self.conn.connect(conf['username'], conf['password'], wait=True)

        return super().__init__()


    def emit(self, record):
        '''
        Send the message to the destination endpoint.
        '''
        self.conn.send(destination=self.conf['destination'],
                body=self.format(record))

