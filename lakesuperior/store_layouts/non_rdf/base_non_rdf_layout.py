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


    ## PROTECTED METHODS ##

    def _path(self, hash):
        '''
        Generate the resource path splitting the resource checksum according to
        configuration parameters.

        @param hash (string) The resource hash.
        '''
        bl = self._conf['pairtree_branch_length']
        bc = self._conf['pairtree_branches']
        term = len(hash) if bc==0 else min(bc*bl, len(hash))

        path = [ hash[i:i+bl] for i in range(0, term, bl) ]

        if bc > 0:
            path.append(hash[:term])
        path.insert(0, self.root)

        return '/'.join(path)
