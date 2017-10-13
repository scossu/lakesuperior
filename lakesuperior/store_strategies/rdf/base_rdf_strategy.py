import logging

from abc import ABCMeta, abstractmethod

from flask import request
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.term import URIRef

from lakesuperior.connectors.graph_store_connector import GraphStoreConnector
from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm


class ResourceExistsError(RuntimeError):
    '''Thrown when a resource is being created for an existing URN.'''
    pass



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

    - Methods starting with `get_` return a graph.
    - Methods starting with `list_` return a list, tuple or set of URIs.
    - Methods starting with `select_` return a list or tuple with table-like
      data such as from a SELECT statement.
    - Methods starting with `ask_` return a boolean value.
    '''

    UNION_GRAPH_URI = URIRef('urn:x-arq:UnionGraph') # This is Fuseki-specific

    _logger = logging.getLogger(__module__)


    ## MAGIC METHODS ##

    def __init__(self):
        self.conn = GraphStoreConnector()
        self.ds = self.conn.ds


    ## PUBLIC METHODS ##

    @abstractmethod
    def ask_rsrc_exists(self, urn):
        '''Return whether the resource exists.

        @param uuid Resource UUID.

        @retrn boolean
        '''
        pass


    @abstractmethod
    def get_rsrc(self, urn):
        '''Get the copy of a resource graph.

        @param uuid Resource UUID.

        @retrn rdflib.resource.Resource
        '''
        pass


    @abstractmethod
    def create_or_replace_rsrc(self, urn, data, commit=True):
        '''Create a resource graph in the main graph if it does not exist.

        If it exists, replace the existing one retaining some special
        properties.
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


    def list_containment_statements(self, urn):
        q = '''
        SELECT ?container ?contained {
          {
            ?s ldp:contains ?contained .
          } UNION {
            ?container ldp:contains ?s .
          }
        }
        '''
        return self.ds.query(q, initBindings={'s' : urn})


    def uuid_to_uri(self, uuid):
        '''Convert a UUID to a URI.

        @return URIRef
        '''
        return URIRef('{}rest/{}'.format(request.host_url, uuid))


    def localize_string(self, s):
        '''Convert URIs into URNs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(
            request.host_url + 'rest/',
            str(nsc['fcres'])
        )


    def globalize_string(self, s):
        '''Convert URNs into URIs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(
            str(nsc['fcres']),
            request.host_url + 'rest/'
        )


    def globalize_term(self, urn):
        '''Convert an URN into an URI using the application base URI.

        @param rdflib.term.URIRef urn Input URN.

        @return rdflib.term.URIRef
        '''
        return URIRef(self.globalize_string(str(urn)))


    def globalize_triples(self, g):
        '''Convert all URNs in a resource or graph into URIs using the
        application base URI.

        @param rdflib.Graph | rdflib.resource.Resource g Input graph.

        @return rdflib.Graph | rdflib.resource.Resource The same class as the
        input value.
        '''
        if isinstance(g, Resource):
            return self._globalize_graph(g.graph).resource(
                    self.globalize_term(g.identifier))
        elif isinstance (g, Graph):
            return self._globalize_graph(g)
        else:
            raise TypeError('Not a valid input type: {}'.format(g))


    def _globalize_graph(self, g):
        '''Globalize a graph.'''
        q = '''
        CONSTRUCT {{ ?s ?p ?o . }} WHERE {{
          ?s ?p ?o .
          {{ FILTER STRSTARTS(str(?s), "{0}") . }}
          UNION
          {{ FILTER STRSTARTS(str(?o), "{0}") . }}
        }}'''.format(nsc['fcres'])
        flt_g = g.query(q)

        for t in flt_g:
            print('Triple: {}'.format(t))
            global_s = self.globalize_term(t[0])
            global_o = self.globalize_term(t[2]) \
                    if isinstance(t[2], URIRef) \
                    else t[2]
            g.remove(t)
            g.add((global_s, t[1], global_o))

        return g


