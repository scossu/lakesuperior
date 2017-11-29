import logging

from abc import ABCMeta, abstractmethod

from flask import current_app
from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.sync import Stomp


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
            protocol_v = StompSpec.VERSION_1_1
        elif self.conf['protocol'] == '12':
            protocol_v = StompSpec.VERSION_1_2
        else:
            protocol_v = StompSpec.VERSION_1_0

        self.conf
        client_config = StompConfig(
            'tcp://{}:{}'.format(self.conf['host'], self.conf['port']),
            login=self.conf['username'],
            passcode=self.conf['password'],
            version=protocol_v
        )
        self.conn = Stomp(client_config)
        self.conn.connect()

        return super().__init__()


    def emit(self, record):
        '''
        Send the message to the destination endpoint.
        '''
        self.conn.send(destination=self.conf['destination'],
                body=bytes(self.format(record), 'utf-8'))

