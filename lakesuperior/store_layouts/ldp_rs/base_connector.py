import logging
import traceback

from abc import ABCMeta, abstractmethod

from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc


class BaseConnector(metaclass=ABCMeta):
    '''
    Handles the connection and dataset information.

    This is indpendent from the application context (production/test) and can
    be passed any configuration options.
    '''

    UNION_GRAPH_URI = URIRef('urn:x-rdflib:default')

    _logger = logging.getLogger(__name__)

    def __init__(self, location, *args, **kwargs):
        '''
        Initialize the connection to the SPARQL endpoint.

        If `update_ep` is not specified, the store is initialized as read-only.
        '''
        self._init_connection(location, *args, **kwargs)


    @abstractmethod
    def _init_connection(self, location, *args, **kwargs):
        '''
        Interface method. Connection steps go here.
        '''
        pass


    def query(self, q, initBindings=None, nsc=nsc):
        '''
        Perform a SPARQL query on the triplestore.

        This provides non-abstract access, independent from the layout.

        @param q (string) SPARQL query.

        @return rdflib.query.Result
        '''
        #self._logger.debug('Sending SPARQL Query: {}\nBindings: {}'.format(
        #    q, initBindings))
        #self._logger.debug('From:\n{}'.format(
        #    (''.join(traceback.format_stack(limit=5)))))
        return self.ds.query(q, initBindings=initBindings, initNs=nsc)


    def update(self, q, initBindings=None, nsc=nsc):
        '''
        Perform a SPARQL update on the triplestore. This is only needed for
        low-level, optimized operations that are not well performed by the
        higher-level methods provided by RDFLib.

        This provides non-abstract access, independent from the layout.

        @param q (string) SPARQL-UPDATE query.

        @return None
        '''
        #self._logger.debug('Sending SPARQL Update: {}\nBindings: {}'.format(
        #    q, initBindings))
        #self._logger.debug('From:\n{}'.format(
        #    (''.join(traceback.format_stack(limit=5)))))
        return self.ds.query(q, initBindings=initBindings)



