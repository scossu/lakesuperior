import logging

from abc import ABCMeta
from importlib import import_module
from itertools import accumulate
from uuid import uuid4

import arrow

from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF, XSD
from rdflib.term import Literal

from lakesuperior.config_parser import config
from lakesuperior.connectors.filesystem_connector import FilesystemConnector
from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.util.translator import Translator


class ResourceExistsError(RuntimeError):
    '''
    Raised in an attempt to create a resource a URN that already exists and is
    not supposed to.

    This usually surfaces at the HTTP level as a 409.
    '''
    pass



class ResourceNotExistsError(RuntimeError):
    '''
    Raised in an attempt to create a resource a URN that does not exist and is
    supposed to.

    This usually surfaces at the HTTP level as a 404.
    '''
    pass



class InvalidResourceError(RuntimeError):
    '''
    Raised when an invalid resource is found.

    This usually surfaces at the HTTP level as a 409 or other error.
    '''
    pass



def transactional(fn):
    '''
    Decorator for methods of the Ldpr class to handle transactions in an RDF
    store.
    '''
    def wrapper(self, *args, **kwargs):
        try:
            ret = fn(self, *args, **kwargs)
            print('Committing transaction.')
            self.gs.conn.store.commit()
            return ret
        except:
            print('Rolling back transaction.')
            self.gs.conn.store.rollback()
            raise

    return wrapper


def must_exist(fn):
    '''
    Ensures that a method is applied to a stored resource.
    Decorator for methods of the Ldpr class.
    '''
    def wrapper(self, *args, **kwargs):
        if not self.is_stored:
            raise ResourceNotExistsError(
                'Resource #{} not found'.format(self.uuid))
        return fn(self, *args, **kwargs)

    return wrapper


def must_not_exist(fn):
    '''
    Ensures that a method is applied to a resource that is not stored.
    Decorator for methods of the Ldpr class.
    '''
    def wrapper(self, *args, **kwargs):
        if self.is_stored:
            raise ResourceExistsError(
                'Resource #{} already exists.'.format(self.uuid))
        return fn(self, *args, **kwargs)

    return wrapper




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

    FCREPO_PTREE_TYPE = nsc['fedora'].Pairtree
    LDP_NR_TYPE = nsc['ldp'].NonRDFSource
    LDP_RS_TYPE = nsc['ldp'].RDFSource

    _logger = logging.getLogger(__module__)

    store_strategy = config['application']['store']['ldp_rs']['strategy']

    ## MAGIC METHODS ##

    def __init__(self, uuid):
        '''Instantiate an in-memory LDP resource that can be loaded from and
        persisted to storage.

        Persistence is done in this class. None of the operations in the store
        strategy should commit an open transaction. Methods are wrapped in a
        transaction by using the `@transactional` decorator.

        @param uuid (string) UUID of the resource.
        '''
        self.uuid = uuid

        # Dynamically load the store strategy indicated in the configuration.
        store_mod = import_module(
                'lakesuperior.store_strategies.rdf.{}'.format(
                        self.store_strategy))
        self._rdf_store_cls = getattr(store_mod, Translator.camelcase(
                self.store_strategy))
        self.gs = self._rdf_store_cls(self.urn)

        # Same thing coud be done for the filesystem store strategy, but we
        # will keep it simple for now.
        self.fs = FilesystemConnector()


    @property
    def urn(self):
        '''
        The internal URI (URN) for the resource as stored in the triplestore.
        This is a URN that needs to be converted to a global URI for the REST
        API.

        @return rdflib.URIRef
        '''
        return nsc['fcres'][self.uuid]


    @property
    def uri(self):
        '''
        The URI for the resource as published by the REST API.

        @return rdflib.URIRef
        '''
        return Translator.uuid_to_uri(self.uuid)


    @property
    def rsrc(self):
        '''
        The RDFLib resource representing this LDPR. This is a copy of the
        stored data if present, and what gets passed to most methods of the
        store strategy methods.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_rsrc'):
            self._rsrc = self.gs.rsrc

        return self._rsrc


    @property
    def is_stored(self):
        return self.gs.ask_rsrc_exists()


    @property
    def types(self):
        '''All RDF types.

        @return generator
        '''
        if not hasattr(self, '_types'):
            self._types = set(self.rsrc[RDF.type])
        return self._types


    @property
    def ldp_types(self):
        '''The LDP types.

        @return set(rdflib.term.URIRef)
        '''
        if not hasattr(self, '_ldp_types'):
            self._ldp_types = set()
            for t in self.types:
                if t.qname()[:4] == 'ldp:':
                    self._ldp_types.add(t)
        return self._ldp_types


    @property
    def containment(self):
        if not hasattr(self, '_containment'):
            q = '''
            SELECT ?container ?contained {
              {
                ?s ldp:contains ?contained .
              } UNION {
                ?container ldp:contains ?s .
              }
            }
            '''
            qres = self.rsrc.graph.query(q, initBindings={'s' : self.urn})

            # There should only be one container.
            for t in qres:
                if t[0]:
                    container = self.gs.ds.resource(t[0])

            contains = ( self.gs.ds.resource(t[1]) for t in qres if t[1] )

            self._containment = {
                    'container' : container, 'contains' : contains}

        return self._containment


    @containment.deleter
    def containment(self):
        '''
        Reset containment variable when changing containment triples.
        '''
        del self._containment


    @property
    def container(self):
        return self.containment['container']


    @property
    def contains(self):
        return self.containment['contains']


    ## STATIC & CLASS METHODS ##

    @classmethod
    def inst(cls, uuid):
        '''
        Fatory method that creates and returns an instance of an LDPR subclass
        based on information that needs to be queried from the underlying
        graph store.

        This is used with retrieval methods for resources that already exist.

        @param uuid UUID of the instance.
        '''
        gs = cls.load_gs_static(cls, uuid)
        rdf_types = gs.rsrc[nsc['res'][uuid] : RDF.type]

        for t in rdf_types:
            if t == cls.LDP_NR_TYPE:
                return LdpNr(uuid)
            if t == cls.LDP_RS_TYPE:
                return LdpRs(uuid)

        raise ValueError('Resource #{} does not exist or does not have a '
                'valid LDP type.'.format(uuid))


    @classmethod
    def load_gs_static(cls, uuid=None):
        '''
        Dynamically load the store strategy indicated in the configuration.
        This essentially replicates the init() code in a static context.
        '''
        store_mod = import_module(
                'lakesuperior.store_strategies.rdf.{}'.format(
                        cls.store_strategy))
        rdf_store_cls = getattr(store_mod, Translator.camelcase(
                cls.store_strategy))
        return rdf_store_cls(uuid)


    @classmethod
    def inst_for_post(cls, parent_uuid=None, slug=None):
        '''
        Validate conditions to perform a POST and return an LDP resource
        instancefor using with the `post` method.

        This may raise an exception resulting in a 404 if the parent is not
        found or a 409 if the parent is not a valid container.
        '''
        # Shortcut!
        if not slug and not parent_uuid:
            return cls(str(uuid4()))

        gs = cls.load_gs_static()
        parent_rsrc = Resource(gs.ds, nsc['fcres'][parent_uuid])

        # Set prefix.
        if parent_uuid:
            parent_exists = gs.ask_rsrc_exists(parent_rsrc)
            if not parent_exists:
                raise ResourceNotExistsError('Parent not found: {}.'
                        .format(parent_uuid))

            if nsc['ldp'].Container not in gs.rsrc.values(RDF.type):
                raise InvalidResourceError('Parent {} is not a container.'
                       .format(parent_uuid))

            pfx = parent_uuid + '/'
        else:
            pfx = ''

        # Create candidate UUID and validate.
        if slug:
            cnd_uuid = pfx + slug
            cnd_rsrc = Resource(gs.ds, nsc['fcres'][cnd_uuid])
            if gs.ask_rsrc_exists(cnd_rsrc):
                return cls(pfx + str(uuid4()))
            else:
                return cls(cnd_uuid)
        else:
            return cls(pfx + str(uuid4()))


    ## LDP METHODS ##

    def head(self):
        '''
        Return values for the headers.
        '''
        headers = self.gs.headers

        for t in self.ldp_types:
            headers['Link'].append('{};rel="type"'.format(t.identifier.n3()))

        return headers


    def get(self, inbound=False):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_GET
        '''
        try:
            g = self.gs.out_graph(inbound)
        except ResultException:
            # RDFlib bug? https://github.com/RDFLib/rdflib/issues/775
            raise ResourceNotExistsError()

        return Translator.globalize_rsrc(g)


    @transactional
    def post(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

        Perform a POST action after a valid resource URI has been found.
        '''
        g = Graph()

        g.parse(data=data, format=format, publicID=self.urn)

        self.gs.create_rsrc(g)

        self._set_containment_rel()


    @transactional
    def put(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT
        '''
        g = Graph()
        g.parse(data=data, format=format, publicID=self.urn)

        self.gs.create_or_replace_rsrc(g)

        self._set_containment_rel()


    @transactional
    @must_exist
    def delete(self):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_DELETE
        '''
        self.gs.delete_rsrc(self.urn)


    ## PROTECTED METHODS ##

    def _set_containment_rel(self):
        '''Find the closest parent in the path indicated by the UUID and
        establish a containment triple.

        E.g.

        - If only urn:fcres:a (short: a) exists:
          - If a/b/c/d is being created, a becomes container of a/b/c/d. Also,
            pairtree nodes are created for a/b and a/b/c.
          - If e is being created, the root node becomes container of e.
        '''
        if '/' in self.uuid:
            # Traverse up the hierarchy to find the parent.
            #candidate_parent_urn = self._find_first_ancestor()
            #cparent = self.gs.ds.resource(candidate_parent_urn)
            cparent_uri = self._find_parent_or_create_pairtree(self.uuid)

            # Reroute possible containment relationships between parent and new
            # resource.
            #self._splice_in(cparent)
            if cparent_uri:
                self.gs.ds.add((cparent_uri, nsc['ldp'].contains,
                        self.rsrc.identifier))
        else:
            self.rsrc.graph.add((nsc['fcsystem'].root, nsc['ldp'].contains,
                    self.rsrc.identifier))
        # If a resource has no parent and should be parent of the new resource,
        # add the relationship.
        #for child_uri in self.find_lost_children():
        #    self.rsrc.add(nsc['ldp'].contains, child_uri)


    def _find_parent_or_create_pairtree(self, uuid):
        '''
        Check the path-wise parent of the new resource. If it exists, return
        its URI. Otherwise, create pairtree resources up the path until an
        actual resource or the root node is found.

        @return rdflib.term.URIRef
        '''
        path_components = uuid.split('/')

        if len(path_components) < 2:
            return None

        # Build search list, e.g. for a/b/c/d/e would be a/b/c/d, a/b/c, a/b, a
        self._logger.info('Path components: {}'.format(path_components))
        fwd_search_order = accumulate(
            list(path_components)[:-1],
            func=lambda x,y : x + '/' + y
        )
        rev_search_order = reversed(list(fwd_search_order))

        cur_child_uri = nsc['fcres'][uuid]
        for cparent_uuid in rev_search_order:
            cparent_uri = nsc['fcres'][cparent_uuid]

            # @FIXME A bit ugly. Maybe we should use a Pairtree class.
            if self._rdf_store_cls(cparent_uri).ask_rsrc_exists():
                return cparent_uri
            else:
                self._create_pairtree(cparent_uri, cur_child_uri)
                cur_child_uri = cparent_uri

        return None


    #def _find_first_ancestor(self):
    #    '''
    #    Find by logic and triplestore queries the first existing resource by
    #    traversing a path hierarchy upwards.

    #    @return rdflib.term.URIRef
    #    '''
    #    path_components = self.uuid.split('/')

    #    if len(path_components) < 2:
    #        return None

    #    # Build search list, e.g. for a/b/c/d/e would be a/b/c/d, a/b/c, a/b, a
    #    search_order = accumulate(
    #        reversed(search_order)[1:],
    #        func=lambda x,y : x + '/' + y
    #    )

    #    for cmp in search_order:
    #        if self.gs.ask_rsrc_exists(ns['fcres'].cmp):
    #            return urn
    #        else:
    #            self._create_pairtree_node(cmp)

    #    return None


    def _create_pairtree(self, uri, child_uri):
        '''
        Create a pairtree node with a containment statement.

        This is the default fcrepo4 behavior and probably not the best one, but
        we are following it here.

        If a resource such as `fcres:a/b/c` is created, and neither fcres:a or
        fcres:a/b exists, we have to create pairtree nodes in order to maintain
        the containment chain.

        This way, both fcres:a and fcres:a/b become thus containers of
        fcres:a/b/c, which may be confusing.
        '''
        g = Graph()
        g.add((uri, RDF.type, nsc['fedora'].Pairtree))
        g.add((uri, RDF.type, nsc['ldp'].Container))
        g.add((uri, RDF.type, nsc['ldp'].BasicContainer))
        g.add((uri, RDF.type, nsc['ldp'].RDFSource))
        g.add((uri, nsc['ldp'].contains, child_uri))
        if '/' not in str(uri):
            g.add((nsc['fcsystem'].root, nsc['ldp'].contains, uri))

        self.gs.create_rsrc(g)



    #def _splice_in(self, parent):
    #    '''
    #    Insert the new resource between a container and its child.

    #    If a resource is inserted between two resources that already have a
    #    containment relationship, e.g. inserting `<a/b>` where
    #    `<a> ldp:contains <a/b/c>` exists, the existing containment
    #    relationship must be broken in order to insert the resource in between.

    #    NOTE: This method only removes the containment relationship between the
    #    old parent (`<a>` in the example above) and old child (`<a/b/c>`) and
    #    sets a new one between the new parent and child (`<a/b>` and
    #    `<a/b/c>`). The relationship between `<a>` and `<a/b>` is set
    #    separately.

    #    @param rdflib.resource.Resource parent The parent resource. This
    #    includes the root node.
    #    '''
    #    # For some reason, initBindings (which adds a VALUES statement in the
    #    # query) does not work **just for `?new`**. `BIND` is necessary along
    #    # with a format() function.
    #    q = '''
    #    SELECT ?child {{
    #      ?p ldp:contains ?child .
    #      FILTER ( ?child != <{}> ) .
    #      FILTER STRSTARTS(str(?child), "{}") .
    #    }}
    #    LIMIT 1
    #    '''.format(self.urn)
    #    qres = self.rsrc.graph.query(q, initBindings={'p' : parent.identifier})

    #    if not qres:
    #        return

    #    child_urn = qres.next()[0]

    #    parent.remove(nsc['ldp'].contains, child_urn)
    #    self.src.add(nsc['ldp'].contains, child_urn)


    #def find_lost_children(self):
    #    '''
    #    If the parent was created after its children and has to find them!
    #    '''
    #    q = '''
    #    SELECT ?child {




class LdpRs(Ldpr):
    '''LDP-RS (LDP RDF source).

    Definition: https://www.w3.org/TR/ldp/#ldprs
    '''
    base_types = {
        nsc['ldp'].RDFSource
    }

    @transactional
    @must_exist
    def patch(self, data):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH
        '''
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        self.gs.patch_rsrc(self.urn, data, ts)

        self.gs.ds.add((self.urn, nsc['fedora'].lastUpdated, ts))
        self.gs.ds.add((self.urn, nsc['fedora'].lastUpdatedBy,
                Literal('BypassAdmin')))


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



