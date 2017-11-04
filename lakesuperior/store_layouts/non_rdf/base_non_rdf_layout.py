import logging

from abc import ABCMeta, abstractmethod

from lakesuperior.config_parser import config


class BaseNonRdfLayout(metaclass=ABCMeta):
    '''
    Abstract class for setting the non-RDF (bitstream) store layout.
    '''

    _conf = config['application']['store']['ldp_nr']
    _logger = logging.getLogger(__name__)


    def __init__(self):
        '''
        Initialize the base non-RDF store layout.
        '''
        self.root = self._conf['path']


    ## INTERFACE METHODS ##

    @abstractmethod
    def persist(self, file):
        '''
        Store the stream in the designated persistence layer for this layout.
        '''
        pass
