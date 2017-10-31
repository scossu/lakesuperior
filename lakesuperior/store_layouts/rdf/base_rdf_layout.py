import logging

from abc import ABCMeta, abstractmethod

from rdflib import Dataset, Graph
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

from lakesuperior.config_parser import config
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm


def needs_rsrc(fn):
    '''
    Decorator for methods that cannot be called without `self.rsrc` set.
    '''
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, 'rsrc') or self.rsrc is None:
            raise TypeError(
                'This method must be called by an instance with `rsrc` set.')

        return fn(self, *args, **kwargs)

    return wrapper



class BaseRdfLayout(metaclass=ABCMeta):
    '''
    This class exposes an interface to build graph store layouts. It also
    provides the baics of the triplestore connection.

    Some store layouts are provided. New ones aimed at specific uses
    and optimizations of the repository may be developed by extending this
    class and implementing all its abstract methods.

    A layout is implemented via application configuration. However, once
    contents are ingested in a repository, changing a layout will most likely
    require a migration.

    The custom layout must be in the lakesuperior.store_layouts.rdf
    package and the class implementing the layout must be called
    `StoreLayout`. The module name is the one defined in the app
    configuration.

    E.g. if the configuration indicates `simple_layout` the application will
    look for
    `lakesuperior.store_layouts.rdf.simple_layout.SimpleLayout`.

    Some method naming conventions:

    - Methods starting with `get_` return a resource.
    - Methods starting with `list_` return an iterable or generator of URIs.
    - Methods starting with `select_` return an iterable or generator with
      table-like data such as from a SELECT statement.
    - Methods starting with `ask_` return a boolean value.
    '''

    ROOT_NODE_URN = nsc['fcsystem'].root
    # N.B. This is Fuseki-specific.
    UNION_GRAPH_URI = URIRef('urn:x-arq:UnionGraph')

    RES_CREATED = '_created_'
    RES_UPDATED = '_updated_'

    _conf = config['application']['store']['ldp_rs']
    _logger = logging.getLogger(__name__)

    query_ep = _conf['webroot'] + _conf['query_ep']
    update_ep = _conf['webroot'] + _conf['update_ep']


    ## MAGIC METHODS ##

    def __init__(self, urn=None):
        '''Initialize the graph store and a layout.

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

        The layout can be initialized with a URN to make resource-centric
        operations simpler. However, for generic queries, urn can be None and
        no `self.rsrc` is assigned. In this case, some methods (the ones
        decorated by `@needs_rsrc`) will not be available.
        '''
        self.ds = Dataset(self.store, default_union=True)
        self.ds.namespace_manager = nsm
        self._base_urn = urn


    @property
    def store(self):
        if not hasattr(self, '_store') or not self._store:
            self._store = SPARQLUpdateStore(
                    queryEndpoint=self.query_ep,
                    update_endpoint=self.update_ep,
                    autocommit=False,
                    dirty_reads=True)

        return self._store


    @property
    def base_urn(self):
        '''
        The base URN for the current resource being handled.

        This value is only here for convenience. It does not preclude one from
        using an instance of this class with more than one subject.
        '''
        return self._base_urn


    @property
    def rsrc(self):
        '''
        Reference to a live data set that can be updated. This exposes the
        whole underlying triplestore structure and is used to update a
        resource.
        '''
        if self.base_urn is None:
            return None
        return self.ds.resource(self.base_urn)


    @property
    @abstractmethod
    @needs_rsrc
    def headers(self):
        '''
        Return a dict with information for generating HTTP headers.

        @retun dict
        '''
        pass


    ## PUBLIC METHODS ##

    def query(self, q, initBindings=None, nsc=nsc):
        '''
        Perform a SPARQL query on the triplestore.

        This should provide non-abstract access, independent from the layout,
        therefore it should not be overridden by individual layouts.

        @param q (string) SPARQL query.

        @return rdflib.query.Result
        '''
        self._logger.debug('Sending SPARQL query: {}'.format(q))
        return self.ds.query(q, initBindings=initBindings, initNs=nsc)


    def update(self, q, initBindings=None, nsc=nsc):
        '''
        Perform a SPARQL update on the triplestore.

        This should provide non-abstract access, independent from the layout,
        therefore it should not be overridden by individual layouts.

        @param q (string) SPARQL-UPDATE query.

        @return None
        '''
        self._logger.debug('Sending SPARQL update: {}'.format(q))
        return self.ds.query(q, initBindings=initBindings, initNs=nsc)


    ## INTERFACE METHODS ##

    # Implementers of custom layouts should look into these methods to
    # implement.

    @abstractmethod
    def extract_imr(self, uri=None, graph=None, inbound=False):
        '''
        Extract an in-memory resource based on the copy of a graph on a subject.

        @param uri (URIRef) Resource URI.
        @param graph (rdflib.term.URIRef | set(rdflib.graphURIRef)) The graph
        to extract from. This can be an URI for a single graph, or a list of
        graph URIs in which case an aggregate graph will be used.
        @param inbound (boolean) Whether to pull triples that have the resource
        URI as their object.
        '''
        pass


    @abstractmethod
    @needs_rsrc
    def out_rsrc(self, srv_mgd=True, inbound=False, embed_children=False):
        '''
        Graph obtained by querying the triplestore and adding any abstraction
        and filtering to make up a graph that can be used for read-only,
        API-facing results. Different layouts can implement this in very
        different ways, so it is an abstract method.

        @return rdflib.resource.Resource
        '''
        pass



    @abstractmethod
    def ask_rsrc_exists(self, uri=None):
        '''
        Ask if a resource exists (is stored) in the graph store.

        @param uri (rdflib.term.URIRef) If this is provided, this method
        will look for the specified resource. Otherwise, it will look for the
        default resource. If this latter is not specified, the result is False.

        @return boolean
        '''
        pass


    @abstractmethod
    @needs_rsrc
    def create_or_replace_rsrc(self, urn, data, commit=True):
        '''Create a resource graph in the main graph if it does not exist.

        If it exists, replace the existing one retaining the creation date.
        '''
        pass


    @abstractmethod
    @needs_rsrc
    def create_rsrc(self, urn, data, commit=True):
        '''Create a resource graph in the main graph.

        If the resource exists, raise an exception.
        '''
        pass


    @abstractmethod
    @needs_rsrc
    def patch_rsrc(self, urn, data, commit=False):
        '''
        Perform a SPARQL UPDATE on a resource.
        '''
        pass


    @abstractmethod
    @needs_rsrc
    def modify_rsrc(self, remove, add):
        '''
        Adds and/or removes triples from a graph.

        @param remove (rdflib.Graph) Triples to be removed.
        @param add (rdflib.Graph) Triples to be added.
        '''
        pass


    @abstractmethod
    @needs_rsrc
    def delete_rsrc(self, urn, commit=True):
        pass
