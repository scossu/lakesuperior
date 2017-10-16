import logging

from abc import ABCMeta, abstractmethod

from flask import request
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.term import URIRef

from lakesuperior.connectors.graph_store_connector import GraphStoreConnector
from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm


class BaseRdfStrategy(metaclass=ABCMeta):
    '''
    This class exposes an interface to build graph store strategies.

    Some store strategies are provided. New ones aimed at specific uses
    and optimizations of the repository may be developed by extending this
    class and implementing all its abstract methods.

    A strategy is implemented via application configuration. However, once
    contents are ingested in a repository, changing a strategy will most likely
    require a migration.

    The custom strategy must be in the lakesuperior.store_strategies.rdf
    package and the class implementing the strategy must be called
    `StoreStrategy`. The module name is the one defined in the app
    configuration.

    E.g. if the configuration indicates `simple_strategy` the application will
    look for
    `lakesuperior.store_strategies.rdf.simple_strategy.SimpleStrategy`.

    Some method naming conventions:

    - Methods starting with `get_` return a resource.
    - Methods starting with `list_` return an iterable or generator of URIs.
    - Methods starting with `select_` return an iterable or generator with
      table-like data such as from a SELECT statement.
    - Methods starting with `ask_` return a boolean value.
    '''

    UNION_GRAPH_URI = URIRef('urn:x-arq:UnionGraph') # This is Fuseki-specific

    _logger = logging.getLogger(__module__)


    ## MAGIC METHODS ##

    def __init__(self, urn):
        self.conn = GraphStoreConnector()
        self.ds = self.conn.ds
        self._base_urn = urn


    @property
    def base_urn(self):
        '''
        The base URN for the current resource being handled.

        This value is only here for convenience. It does not preclde from using
        an instance of this class with more than one subject.
        '''
        return self._base_urn


    @property
    def rsrc(self):
        '''
        Reference to a live data set that can be updated. This exposes the
        whole underlying triplestore structure and is used to update a
        resource.
        '''
        return self.ds.resource(self.base_urn)


    @property
    @abstractmethod
    def out_graph(self):
        '''
        Graph obtained by querying the triplestore and adding any abstraction
        and filtering to make up a graph that can be used for read-only,
        API-facing results. Different strategies can implement this in very
        different ways, so it is an abstract method.
        '''
        pass


    ## PUBLIC METHODS ##

    @abstractmethod
    def create_or_replace_rsrc(self, urn, data, commit=True):
        '''Create a resource graph in the main graph if it does not exist.

        If it exists, replace the existing one retaining the creation date.
        '''
        pass


    @abstractmethod
    def create_rsrc(self, urn, data, commit=True):
        '''Create a resource graph in the main graph.

        If the resource exists, raise an exception.
        '''
        pass


    @abstractmethod
    def patch_rsrc(self, urn, data, commit=False):
        '''
        Perform a SPARQL UPDATE on a resource.
        '''
        pass


    @abstractmethod
    def delete_rsrc(self, urn, commit=True):
        pass
