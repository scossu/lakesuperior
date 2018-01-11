from flask import current_app, g
from rdflib.resource import Resource

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.store_layouts.ldp_rs.rsrc_centric_layout import PTREE_GR_URI


class GenericResource:
    '''
    Generic RDF resource.

    This may not have a dedicated named graph.
    '''

    def __init__(self, uid):
        '''
        Initialize a generic resource.
        '''
        self.uid = uid
        self.urn = nsc['fcres'][uid]
        self.rdfly = current_app.rdfly


    @property
    def metadata(self):
        if not hasattr(self, '_metadata'):
            gr = self.rdfly.get_raw(self.urn)
            self._metadata = Resource(gr, self.urn)

        return self._metadata


    @property
    def out_graph(self):
        return self.metadata.graph


    def head(self):
        '''
        No-op to keep consistency with methods that may request this
        without knowing if it is a LDP resource or what else.
        '''
        return {}


    def extract(self, p=None, o=None):
        '''
        Extract an in-memory copy of the resource containing either a
        sub-graph, defined with the `p` and `o` parameters, or the whole
        resource.
        '''
        # @TODO
        pass


class PathSegment(GenericResource):
    '''
    Represent a path segment in a URI.

    A path segment is not an LDP resource, and its metadata should be confined
    to a separate, generic named graph.
    '''
    @property
    def metadata(self):
        if not hasattr(self, '_metadata'):
            gr = self.rdfly.get_raw(self.urn, PTREE_GR_URI)
            self._metadata = Resource(gr, self.urn)

        return self._metadata


    def get(self):
        '''
        Get an RDF representation of the resource.

        Internal URNs are replaced by global URIs using the endpoint webroot.
        The resource has very few triples so no namespace manager is used to
        reduce output size.
        '''
        return g.tbox.globalize_graph(self.out_graph)


