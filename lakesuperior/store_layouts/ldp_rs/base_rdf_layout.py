import logging

from abc import ABCMeta, abstractmethod

from flask import current_app
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.exceptions import ResourceNotExistsError
from lakesuperior.store_layouts.ldp_rs.bdb_connector import BdbConnector
from lakesuperior.store_layouts.ldp_rs.sqlite_connector import SqliteConnector
from lakesuperior.toolbox import Toolbox



class BaseRdfLayout(metaclass=ABCMeta):
    '''
    This class exposes an interface to build graph store layouts. It also
    provides the basics of the triplestore connection.

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

    def __init__(self, conn, config):
        '''Initialize the graph store and a layout.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). Blazegraph supports
        this only in the (currently unreleased) 2.2 branch. It works with Jena,
        which is currently the reference implementation.
        '''
        self.config = config
        self._conn = conn
        self.store = self._conn.store

        #self.UNION_GRAPH_URI = self._conn.UNION_GRAPH_URI
        self.ds = self._conn.ds
        self.ds.namespace_manager = nsm


    ## INTERFACE METHODS ##

    # Implementers of custom layouts should look into these methods to
    # implement.

    @abstractmethod
    def extract_imr(self, uri, strict=True, incl_inbound=False,
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
        raise a `ResourceNotExistsError`; if a tombstone is found, a
        `TombstoneError` is raised. Otherwise, the raw graph is returned.
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


    #@abstractmethod
    def get_version_info(self, urn):
        '''
        Get version information about a resource (`fcr:versions`)
        '''
        pass


    #@abstractmethod
    def get_version(self, urn):
        '''
        Get a historic snapshot (version) of a resource.
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


    #@abstractmethod
    def modify_dataset(self, remove_trp=Graph(), add_trp=Graph(),
            types=set()):
        '''
        Adds and/or removes triples from the persistent data set.

        NOTE: This method can apply to an arbitrary graph including multiple
        resources.

        @param remove_trp (rdflib.Graph) Triples to be removed.
        @param add_trp (rdflib.Graph) Triples to be added.
        @param types (iterable(rdflib.term.URIRef)) RDF types of the resource
        that may be relevant to the layout strategy. These can be anything
        since they are just used to inform the layout and not actually stored.
        If this is an empty set, the merge graph is used.
        '''
        pass


