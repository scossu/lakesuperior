import logging

from abc import ABCMeta, abstractmethod


class StompHandler(logging.StreamHandler):
    '''
    Send messages to a remote queue broker using the STOMP protocol.

    This module is named and configured separately from
    standard logging for clarity about its scope: while logging has an
    informational purpose, this module has a functional one.
    '''
    def __init__(self, ep):
        self.ep = ep
        super().__init__()


    def emit(self, record):
        '''
        Send the message to the destination endpoint.
        '''
        return self.format(record)


