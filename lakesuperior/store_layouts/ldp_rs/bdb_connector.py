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

    def _init_connection(self, location):
        '''
        Initialize the connection to the BerkeleyDB (Sleepycat) store.

        Also open the store, which must be closed by the __del__ method.
        '''
        #self.store = plugin.get('Sleepycat', Store)(
        #        identifier=URIRef('urn:fcsystem:lsup'))
        self.ds = Dataset('Sleepycat', default_union=True)
        self.store = self.ds.store
        self.ds.open(location, create=True)


    def __del__(self):
        '''
        Close store connection.
        '''
        self.ds.close(commit_pending_transaction=False)
