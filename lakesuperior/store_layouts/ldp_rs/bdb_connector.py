import logging

from rdflib import Dataset, plugin
from rdflib.store import Store
from rdflib.term import URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLStore, SPARQLUpdateStore
from SPARQLWrapper.Wrapper import POST

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.store_layouts.ldp_rs.base_connector import BaseConnector


class BdbConnector(BaseConnector):
    '''
    Handles the connection and dataset information.

    This is indpendent from the application context (production/test) and can
    be passed any configuration options.
    '''

    _logger = logging.getLogger(__name__)

    def _init_connection(self, path):
        '''
        Initialize the connection to the SPARQL endpoint.

        If `update_ep` is not specified, the store is initialized as read-only.
        '''
        self.store = plugin.get('Sleepycat', Store)(
                identifier=URIRef('urn:fcsystem:lsup'))
        self.store.open(path, create=True)
        self.ds = Dataset(self.store, default_union=True)


    def __del__(self):
        self.store.close()
