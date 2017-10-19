import logging

from abc import ABCMeta, abstractmethod

from flask import request
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.term import URIRef

from lakesuperior.connectors.graph_store_connector import GraphStoreConnector
from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm


def needs_rsrc(fn):
    '''
    Decorator for methods that cannot be called without `self.rsrc` set.
    '''
    def wrapper(self, *args, **kwargs):
        if not isset(self, '_rsrc') or self._rsrc is None:
            raise TypeError(
                'This method must be called by an instance with `rsrc` set.')

        return fn(self, *args, **kwargs)

    return wrapper



class BaseRdfLayout(metaclass=ABCMeta):
    '''
    This class exposes an interface to build graph store layouts.

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

    # N.B. This is Fuseki-specific.
    UNION_GRAPH_URI = URIRef('urn:x-arq:UnionGraph')

    _logger = logging.getLogger(__module__)


    ## MAGIC METHODS ##

    def __init__(self, urn=None):
        '''
        The layout can be initialized with a URN to make resource-centric
        operations simpler. However, for generic queries, urn can be None and
        no `self.rsrc` is assigned. In this case, some methods will not be
        available.
        '''
        self.conn = GraphStoreConnector()
        self.ds = self.conn.ds
        self._base_urn = urn


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


    @abstractmethod
    @needs_rsrc
    def out_graph(self, srv_mgd=True, inbound=False, embed_children=False):
        '''
        Graph obtained by querying the triplestore and adding any abstraction
        and filtering to make up a graph that can be used for read-only,
        API-facing results. Different layouts can implement this in very
        different ways, so it is an abstract method.
        '''
        pass



    @abstractmethod
    def ask_rsrc_exists(self):
        '''
        Ask if a resource exists (is stored) in the graph store.

        @param rsrc (rdflib.resource.Resource) If this is provided, this method
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
    def delete_rsrc(self, urn, commit=True):
        pass
