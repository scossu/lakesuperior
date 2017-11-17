import logging

from rdflib import Dataset
from rdflib.plugins.stores.sparqlstore import SPARQLStore, SPARQLUpdateStore
from SPARQLWrapper.Wrapper import POST

from lakesuperior.dictionaries.namespaces import ns_collection as nsc


class GraphStoreConnector:
    '''
    Handles the connection and dataset information.

    This is indpendent from the application context (production/test) and can
    be passed any configuration options.
    '''

    _logger = logging.getLogger(__name__)

    def __init__(self, query_ep, update_ep=None):
        if update_ep:
            self.store = SPARQLUpdateStore(
                    queryEndpoint=query_ep,
                    update_endpoint=update_ep,
                    autocommit=False,
                    dirty_reads=True)

            self.readonly = False
        else:
            self.store = SPARQLStore(query_ep, default_query_method=POST)
            self.readonly = True

        self.ds = Dataset(self.store, default_union=True)


    def query(self, q, initBindings=None, nsc=nsc):
        '''
        Perform a SPARQL query on the triplestore.

        This provides non-abstract access, independent from the layout.

        @param q (string) SPARQL query.

        @return rdflib.query.Result
        '''
        self._logger.debug('Sending SPARQL query: {}'.format(q))
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
        self._logger.debug('Sending SPARQL update: {}'.format(q))
        return self.ds.query(q, initBindings=initBindings, initNs=nsc)



