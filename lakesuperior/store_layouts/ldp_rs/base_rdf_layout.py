import logging

from abc import ABCMeta, abstractmethod

from rdflib.namespace import RDF
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.exceptions import ResourceNotExistsError
from lakesuperior.store_layouts.ldp_rs.graph_store_connector import \
        GraphStoreConnector
from lakesuperior.toolbox import Toolbox



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

    _logger = logging.getLogger(__name__)


    ## MAGIC METHODS ##

    def __init__(self, config):
        '''Initialize the graph store and a layout.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). Blazegraph supports
        this only in the (currently unreleased) 2.2 branch. It works with Jena,
        which is currently the reference implementation.
        '''
        self.config = config
        self._conn = GraphStoreConnector(
                query_ep=config['webroot'] + config['query_ep'],
                update_ep=config['webroot'] + config['update_ep'])

        self.store = self._conn.store

        self.ds = self._conn.ds
        self.ds.namespace_manager = nsm


    ## INTERFACE METHODS ##

    # Implementers of custom layouts should look into these methods to
    # implement.

    @abstractmethod
    def extract_imr(self, uri, strict=False, incl_inbound=False,
                incl_children=True, embed_children=False, incl_srv_mgd=True):
        '''
        Extract an in-memory resource from the dataset restricted to a subject.

        some filtering operations are carried out in this method for
        performance purposes (e.g. `incl_children` and `embed_children`, i.e.
        the IMR will never have those properties). Others, such as
        server-managed triples, are kept in the IMR until they are filtered out
        when the graph is output with `Ldpr.out_graph`.

        @param uri (URIRef) Resource URI.
        @param strict (boolean) If set to True, an empty result graph will
        raise a `ResourceNotExistsError`.
        @param incl_inbound (boolean) Whether to pull triples that have the
        resource URI as their object.
        @param incl_children (boolean) Whether to include all children
        indicated by `ldp:contains`. This is only effective if `incl_srv_mgd`
        is True.
        @param embed_children (boolean) If this and `incl_children` are True,
        the full graph is retrieved for each of the children.
        @param incl_srv_mgd (boolean) Whether to include server-managed
        triples.
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
    def modify_dataset(self, remove_trp=[], add_trp=[], metadata={}):
        '''
        Adds and/or removes triples from the graph.

        This is a crucial point for messaging. Any write operation on the RDF
        store that needs to be notified must be performed by invoking this
        method.

        NOTE: This method can apply to multiple resources. However, if
        distinct resources are undergoing different operations (e.g. resource A
        is being deleted and resource B is being updated) this method must be
        called once for each operation.

        @param remove_trp (Iterable) Triples to be removed. This can be a graph
        @param add_trp (Iterable) Triples to be added. This can be a graph.
        @param metadata (dict) Metadata related to the operation. At a minimum,
        it should contain the name of the operation (create, update, delete).
        If no metadata are passed, no messages are enqueued.
        '''
        pass


    def _enqueue_event(self, remove_trp, add_trp):
        '''
        Group delta triples by subject and send out to event queue.

        The event queue is stored in the request context and is processed
        after `store.commit()` is called by the `atomic` decorator.
        '''
        remove_grp = groupby(remove_trp, lambda x : x[0])
        remove_dict = { k[0] : k[1] for k in remove_grp }

        add_grp = groupby(add_trp, lambda x : x[0])
        add_dict = { k[0] : k[1] for k in add_grp }

        subjects = set(remove_dict.keys()) | set(add_dict.keys())
        for rsrc_uri in subjects:
            request.changelog.append(
                uri=rsrc_uri,
                ev_type=None,
                time=arrow.utcnow(),
                type=list(imr.graph.subjects(imr.identifier, RDF.type)),
                data=imr.graph,
                metadata={
                    'actor' : imr.value(nsc['fcrepo'].lastModifiedBy),
                }
            )

