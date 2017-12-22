import logging

from abc import ABCMeta

from rdflib import Dataset
from rdflib.term import URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLStore, SPARQLUpdateStore
from SPARQLWrapper.Wrapper import POST

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.store_layouts.ldp_rs.base_connector import BaseConnector


class SparqlConnector(BaseConnector):
    '''
    Handles the connection and dataset information.

    This is indpendent from the application context (production/test) and can
    be passed any configuration options.
    '''

    # N.B. This is Fuseki-specific.
    UNION_GRAPH_URI = URIRef('urn:x-arq:UnionGraph')

    _logger = logging.getLogger(__name__)

    def _init_connection(self, location, query_ep, update_ep=None,
            autocommit=False):
        '''
        Initialize the connection to the SPARQL endpoint.

        If `update_ep` is not specified, the store is initialized as read-only.
        '''
        if update_ep:
            self.store = SPARQLUpdateStore(
                    queryEndpoint=location + query_ep,
                    update_endpoint=location + update_ep,
                    autocommit=autocommit,
                    dirty_reads=not autocommit)

            self.readonly = False
        else:
            self.store = SPARQLStore(
                    location + query_ep, default_query_method=POST)
            self.readonly = True

        self.ds = Dataset(self.store, default_union=True)
