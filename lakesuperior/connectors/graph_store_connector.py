import logging
import uuid

from flask import request
from rdflib import Dataset
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.term import URIRef

from lakesuperior.config_parser import config
from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm


class GraphStoreConnector:
    '''Connector for LDP-RS (RDF Source) resources. Connects to a
    triplestore.
    '''

    _conf = config['application']['store']['ldp_rs']
    _logger = logging.getLogger(__module__)

    query_ep = _conf['webroot'] + _conf['query_ep']
    update_ep = _conf['webroot'] + _conf['update_ep']


    ## MAGIC METHODS ##

    @property
    def store(self):
        if not hasattr(self, '_store') or not self._store:
            self._store = SPARQLUpdateStore(
                    queryEndpoint=self.query_ep,
                    update_endpoint=self.update_ep,
                    autocommit=False,
                    dirty_reads=True)

        return self._store


    def __init__(self):
        '''Initialize the graph store.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). Blazegraph supports
        this only in the (currently) unreleased 2.2 branch. It works with Jena,
        but other considerations would have to be made (e.g. Jena has no REST
        API for handling transactions).

        In a more advanced development phase it could be possible to extend the
        SPARQLUpdateStore class to add non-standard interaction with specific
        SPARQL implementations in order to support ACID features provided
        by them; e.g. Blazegraph's RESTful transaction handling methods.
        '''
        self.ds = Dataset(self.store, default_union=True)
        self.ds.namespace_manager = nsm


    #def __del__(self):
    #    '''Commit pending transactions and close connection.'''
    #    self.store.close(True)


    ## PUBLIC METHODS ##

    def query(self, q, initBindings=None, nsc=nsc):
        '''
        Perform a custom query on the triplestore.

        @param q (string) SPARQL query.

        @return rdflib.query.Result
        '''
        self._logger.debug('Querying SPARQL endpoint: {}'.format(q))
        return self.ds.query(q, initBindings=initBindings, initNs=nsc)


