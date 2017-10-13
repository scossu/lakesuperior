from abc import ABCMeta
from importlib import import_module

import arrow

from rdflib import Graph
from rdflib.namespace import XSD
from rdflib.term import Literal

from lakesuperior.config_parser import config
from lakesuperior.connectors.filesystem_connector import FilesystemConnector
from lakesuperior.core.namespaces import ns_collection as nsc


class Ldpr(metaclass=ABCMeta):
    '''LDPR (LDP Resource).

    Definition: https://www.w3.org/TR/ldp/#ldpr-resource

    This class and related subclasses contain the implementation pieces of
    the vanilla LDP specifications. This is extended by the
    `lakesuperior.fcrepo.Resource` class.

    Inheritance graph: https://www.w3.org/TR/ldp/#fig-ldpc-types

    Note: Even though LdpNr (which is a subclass of Ldpr) handles binary files,
    it still has an RDF representation in the triplestore. Hence, some of the
    RDF-related methods are defined in this class rather than in the LdpRs
    class.

    Convention notes:

    All the methods in this class handle internal UUIDs (URN). Public-facing
    URIs are converted from URNs and passed by these methods to the methods
    handling HTTP negotiation.

    The data passed to the store strategy for processing should be in a graph.
    All conversion from request payload strings is done here.
    '''

    LDP_NR_TYPE = nsc['ldp'].NonRDFSource
    LDP_RS_TYPE = nsc['ldp'].RDFSource

    store_strategy = config['application']['store']['ldp_rs']['strategy']


    ## MAGIC METHODS ##

    def __init__(self, uuid):
        '''Instantiate an in-memory LDP resource that can be loaded from and
        persisted to storage.

        @param uuid (string) UUID of the resource.
        '''
        # Dynamically load the store strategy indicated in the configuration.
        store_mod = import_module(
                'lakesuperior.store_strategies.rdf.{}'.format(
                        self.store_strategy))
        store_cls = getattr(store_mod, self._camelcase(self.store_strategy))
        self.gs = store_cls()

        # Same thing coud be done for the filesystem store strategy, but we
        # will keep it simple for now.
        self.fs = FilesystemConnector()

        self.uuid = uuid


    @property
    def urn(self):
        return nsc['fcres'][self.uuid]


    @property
    def uri(self):
        return self.gs.uuid_to_uri(self.uuid)


    @property
    def types(self):
        '''The LDP types.

        @return tuple(rdflib.term.URIRef)
        '''
        return self.gs.list_types(self.uuid)


    ## LDP METHODS ##

    def get(self):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_GET
        '''
        ret = self.gs.get_rsrc(self.uuid)

        return ret


    def post(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        g = Graph()
        g.parse(data=data, format=format, publicID=self.urn)

        data.add((self.urn, nsc['fedora'].lastUpdated, ts))
        data.add((self.urn, nsc['fedora'].lastUpdatedBy,
                Literal('BypassAdmin')))

        self.gs.create_rsrc(self.urn, g, ts)

        self.gs.conn.store.commit()


    def put(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        g = Graph()
        g.parse(data=data, format=format, publicID=self.urn)

        g.add((self.urn, nsc['fedora'].lastUpdated, ts))
        g.add((self.urn, nsc['fedora'].lastUpdatedBy,
                Literal('BypassAdmin')))

        self.gs.create_or_replace_rsrc(self.urn, g, ts)

        self.gs.conn.store.commit()


    def delete(self):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_DELETE
        '''
        self.gs.delete_rsrc(self.urn, commit=True)


    ## PROTECTED METHODS ##

    def _create_containment_rel(self):
        '''Find the closest parent in the path indicated by the UUID and
        establish a containment triple.

        E.g. If ONLY urn:res:a exist:

        - If a/b is being created, a becomes container of a/b.
        - If a/b/c/d is being created, a becomes container of a/b/c/d.
        - If e is being created, the root node becomes container of e.
          (verify if this is useful or necessary in any way).
        - If only a and a/b/c/d exist, and therefore a contains a/b/c/d, and
        a/b is created:
          - a ceases to be the container of a/b/c/d
          - a becomes container of a/b
          - a/b becomes container of a/b/c/d.
        '''
        if self.gs.list_containment_statements(self.urn):
            pass


    def _camelcase(self, word):
        '''
        Convert a string with underscores with a camel-cased one.

        Ripped from https://stackoverflow.com/a/6425628
        '''
        return ''.join(x.capitalize() or '_' for x in word.split('_'))



class LdpRs(Ldpr):
    '''LDP-RS (LDP RDF source).

    Definition: https://www.w3.org/TR/ldp/#ldprs
    '''
    base_types = {
        nsc['ldp'].RDFSource
    }

    def patch(self, data):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH
        '''
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        self.gs.patch_rsrc(self.urn, data, ts)

        self.gs.ds.add((self.urn, nsc['fedora'].lastUpdated, ts))
        self.gs.ds.add((self.urn, nsc['fedora'].lastUpdatedBy,
                Literal('BypassAdmin')))

        self.gs.conn.store.commit()


class LdpNr(LdpRs):
    '''LDP-NR (Non-RDF Source).

    Definition: https://www.w3.org/TR/ldp/#ldpnr
    '''
    pass



class Ldpc(LdpRs):
    '''LDPC (LDP Container).'''

    def __init__(self, uuid):
        super().__init__(uuid)
        self.base_types.update({
            nsc['ldp'].Container,
        })




class LdpBc(Ldpc):
    '''LDP-BC (LDP Basic Container).'''
    pass



class LdpDc(Ldpc):
    '''LDP-DC (LDP Direct Container).'''

    def __init__(self, uuid):
        super().__init__(uuid)
        self.base_types.update({
            nsc['ldp'].DirectContainer,
        })



class LdpIc(Ldpc):
    '''LDP-IC (LDP Indirect Container).'''

    def __init__(self, uuid):
        super().__init__(uuid)
        self.base_types.update({
            nsc['ldp'].IndirectContainer,
        })



