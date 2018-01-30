import logging

from rdflib import Dataset, plugin
from rdflib.store import Store
from rdflib.term import URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLStore, SPARQLUpdateStore
from SPARQLWrapper.Wrapper import POST

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.store_layouts.ldp_rs.base_connector import BaseConnector

Lmdb = plugin.register('Lmdb', Store,
        'lakesuperior.store_layouts.ldp_rs.lmdb_store', 'LmdbStore')

class LmdbConnector(BaseConnector):
    '''
    Handles the connection with a LMDB store.
    '''

    _logger = logging.getLogger(__name__)

    def _init_connection(self, location):
        '''
        Initialize the connection to the LMDB store and open it.
        '''
        self.store = plugin.get('Lmdb', Store)(location)
        self.ds = Dataset(self.store)


    def __del__(self):
        '''
        Close store connection.
        '''
        self.ds.close(commit_pending_transaction=False)
