import logging

from rdflib import Dataset
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

from lakesuperior.config_parser import config

class GraphStoreConnector:
    '''Connector for LDP-RS (RDF Source) resources. Connects to a
    triplestore.'''

    _conf = config['application']['store']['ldp_rs']
    _logger = logging.getLogger(__module__)


    ## MAGIC METHODS ##

    def __init__(self, method=POST):
        '''Initialize the graph store.

        @param method (string) HTTP method to use for the query. POST is the
        default and recommended value since it places virtually no limitation
        on the query string length.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). This may not be
        viable with the current version of Blazegraph. It would with Fuseki,
        but other considerations would have to be made (e.g. Jena has no REST
        API for handling transactions).
        '''

        self._store = SPARQLUpdateStore(queryEnpdpoint=self._conf['query_ep'],
                update_endpoint=self._conf['update_ep'])
        try:
            self._store.open(self._conf['base_url'])
        except:
            raise RuntimeError('Error opening remote graph store.')
        self.dataset = Dataset(self._store)


    def __del__(self):
        '''Commit pending transactions and close connection.'''
        self._store.close(True)


    ## PUBLIC METHODS ##

    def query(self, q, initNs=None, initBindings=None):
        '''Query the triplestore.

        @param q (string) SPARQL query.

        @return rdflib.query.Result
        '''
        self._logger.debug('Querying SPARQL endpoint: {}'.format(q))
        return self.dataset.query(q, initNs=initNs or nsc,
                initBindings=initBindings)


    def find_by_type(self, type):
        '''Find all resources by RDF type.

        @param type (rdflib.term.URIRef) RDF type to query.
        '''
        return self.query('SELECT ?s {{?s a {} . }}'.format(type.n3()))



