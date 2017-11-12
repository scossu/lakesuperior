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
from lakesuperior.exceptions import ResourceNotExistsError


#def needs_rsrc(fn):
#    '''
#    Decorator for methods that cannot be called without `self.rsrc` set.
#    '''
#    def wrapper(self, *args, **kwargs):
#        if not hasattr(self, 'rsrc') or self.rsrc is None:
#            raise TypeError(
#                'This method must be called by an instance with `rsrc` set.')
#
#        return fn(self, *args, **kwargs)
#
#    return wrapper



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

    conf = config['application']['store']['ldp_rs']
    _logger = logging.getLogger(__name__)

    query_ep = conf['webroot'] + conf['query_ep']
    update_ep = conf['webroot'] + conf['update_ep']


    ## MAGIC METHODS ##

    def __init__(self):
        '''Initialize the graph store and a layout.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). Blazegraph supports
        this only in the (currently) unreleased 2.2 branch. It works with Jena,
        but other considerations would have to be made (e.g. Jena has no REST
        API for handling transactions).
        '''
        self.ds = Dataset(self.store, default_union=True)
        self.ds.namespace_manager = nsm


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
    def protected_pred(self):
        '''
        Predicated that are not deleted from an existing resources when it is
        replaced, e.g. by a PUT operation.
        '''
        return {
            nsc['fcrepo'].created,
            nsc['fcrepo'].createdBy,
            nsc['ldp'].contains,
        }


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

        This should provide low-level access, independent from the layout,
        therefore it should not be overridden by individual layouts.

        @param q (string) SPARQL-UPDATE query.

        @return None
        '''
        self._logger.debug('Sending SPARQL update: {}'.format(q))
        return self.ds.query(q, initBindings=initBindings, initNs=nsc)


    def rsrc(self, urn):
        '''
        Reference to a live data set that can be updated. This exposes the
        whole underlying triplestore structure and is used to update a
        resource.
        '''
        return self.ds.resource(urn)


    def out_rsrc(self, urn):
        '''
        Graph obtained by querying the triplestore and adding any abstraction
        and filtering to make up a graph that can be used for read-only,
        API-facing results. Different layouts can implement this in very
        different ways, so it is an abstract method.

        @return rdflib.resource.Resource
        '''
        imr = self.extract_imr(urn)
        if not len(imr.graph):
            raise ResourceNotExistsError


    def create_or_replace_rsrc(self, imr):
        '''Create a resource graph in the main graph if it does not exist.

        If it exists, replace the existing one retaining the creation date.
        '''
        if self.ask_rsrc_exists(imr.identifier):
            self._logger.info(
                    'Resource {} exists. Removing all outbound triples.'
                    .format(imr.identifier))
            return self.replace_rsrc(imr)
        else:
            return self.create_rsrc(imr)


    ## INTERFACE METHODS ##

    # Implementers of custom layouts should look into these methods to
    # implement.

    @abstractmethod
    def extract_imr(self, uri, strict=False, minimal=False, incl_inbound=False,
                embed_children=False, incl_srv_mgd=True):
        '''
        Extract an in-memory resource based on the copy of a graph on a subject.

        @param uri (URIRef) Resource URI.
        @param strict (boolean) If set to True, an empty result graph will
        raise a `ResourceNotExistsError`.
        @param inbound (boolean) Whether to pull triples that have the resource
        URI as their object.
        '''
        pass


    @abstractmethod
    def ask_rsrc_exists(self, urn):
        '''
        Ask if a resource is stored in the graph store.

        @param uri (rdflib.term.URIRef) The internal URN of the resource to be
        queried.

        @return boolean
        '''
        pass


    @abstractmethod
    def create_rsrc(self, imr):
        '''Create a resource graph in the main graph.

        If the resource exists, raise an exception.
        '''
        pass


    @abstractmethod
    def replace_rsrc(self, imr):
        '''Replace a resource, i.e. delete all the triples and re-add the
        ones provided.

        @param g (rdflib.Graph) Graph to load. It must not contain
        `fcrepo:created` and `fcrepo:createdBy`.
        '''
        pass


    @abstractmethod
    def modify_dataset(self, remove_trp, add_trp):
        '''
        Adds and/or removes triples from the graph.

        NOTE: This is not specific to a resource. The LDP layer is responsible
        for checking that all the +/- triples are referring to the intended
        subject(s).

        @param remove (rdflib.Graph) Triples to be removed.
        @param add (rdflib.Graph) Triples to be added.
        '''
        pass


    @abstractmethod
    def delete_rsrc(self, urn, inbound=True):
        pass



    ## PROTECTED METHODS  ##

    def _set_msg_digest(self):
        '''
        Add a message digest to the current resource.
        '''
        cksum = Digest.rdf_cksum(self.rsrc.graph)
        self.rsrc.set(nsc['premis'].hasMessageDigest,
                URIRef('urn:sha1:{}'.format(cksum)))

