import logging

from abc import ABCMeta
from importlib import import_module
from itertools import accumulate
from uuid import uuid4

from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF, XSD

from lakesuperior.config_parser import config
from lakesuperior.connectors.filesystem_connector import FilesystemConnector
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import InvalidResourceError, \
        ResourceNotExistsError, ServerManagedTermError
from lakesuperior.util.translator import Translator


def transactional(fn):
    '''
    Decorator for methods of the Ldpr class to handle transactions in an RDF
    store.
    '''
    def wrapper(self, *args, **kwargs):
        try:
            ret = fn(self, *args, **kwargs)
            print('Committing transaction.')
            self.rdfly.store.commit()
            return ret
        except:
            print('Rolling back transaction.')
            self.rdfly.store.rollback()
            raise

    return wrapper


def must_exist(fn):
    '''
    Ensures that a method is applied to a stored resource.
    Decorator for methods of the Ldpr class.
    '''
    def wrapper(self, *args, **kwargs):
        if not self.is_stored:
            raise ResourceNotExistsError(self.uuid)
        return fn(self, *args, **kwargs)

    return wrapper


def must_not_exist(fn):
    '''
    Ensures that a method is applied to a resource that is not stored.
    Decorator for methods of the Ldpr class.
    '''
    def wrapper(self, *args, **kwargs):
        if self.is_stored:
            raise ResourceExistsError(self.uuid)
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

    The data passed to the store layout for processing should be in a graph.
    All conversion from request payload strings is done here.
    '''

    FCREPO_PTREE_TYPE = nsc['fcrepo'].Pairtree
    LDP_NR_TYPE = nsc['ldp'].NonRDFSource
    LDP_RS_TYPE = nsc['ldp'].RDFSource

    _logger = logging.getLogger(__name__)

    rdf_store_layout = config['application']['store']['ldp_rs']['layout']

    ## MAGIC METHODS ##

    def __init__(self, uuid):
        '''Instantiate an in-memory LDP resource that can be loaded from and
        persisted to storage.

        Persistence is done in this class. None of the operations in the store
        layout should commit an open transaction. Methods are wrapped in a
        transaction by using the `@transactional` decorator.

        @param uuid (string) UUID of the resource.
        '''
        self.uuid = uuid

        # Dynamically load the store layout indicated in the configuration.
        store_mod = import_module(
                'lakesuperior.store_layouts.rdf.{}'.format(
                        self.rdf_store_layout))
        rdf_store_cls = getattr(store_mod, Translator.camelcase(
                self.rdf_store_layout))

        self._urn = nsc['fcres'][uuid] if self.uuid is not None \
                else rdf_store_cls.ROOT_NODE_URN

        self.rdfly = rdf_store_cls(self._urn)

        # Same thing coud be done for the filesystem store layout, but we
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
        return self._urn

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
        store layout methods.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_rsrc'):
            self._rsrc = self.rdfly.rsrc

        return self._rsrc


    @property
    def is_stored(self):
        return self.rdfly.ask_rsrc_exists()


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
                    container = self.rdfly.ds.resource(t[0])

            contains = ( self.rdfly.ds.resource(t[1]) for t in qres if t[1] )

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
    def load_rdf_layout(cls, uuid=None):
        '''
        Dynamically load the store layout indicated in the configuration.
        This essentially replicates the init() code in a static context.
        '''
        store_mod = import_module(
                'lakesuperior.store_layouts.rdf.{}'.format(
                        cls.rdf_store_layout))
        rdf_layout_cls = getattr(store_mod, Translator.camelcase(
                cls.rdf_store_layout))
        return rdf_layout_cls(uuid)


    @classmethod
    def readonly_inst(cls, uuid):
        '''
        Fatory method that creates and returns an instance of an LDPR subclass
        based on information that needs to be queried from the underlying
        graph store.

        This is used with retrieval methods for resources that already exist.

        @param uuid UUID of the instance.
        '''
        rdfly = cls.load_rdf_layout(cls, uuid)
        rdf_types = rdfly.rsrc[nsc['res'][uuid] : RDF.type]

        for t in rdf_types:
            if t == cls.LDP_NR_TYPE:
                return LdpNr(uuid)
            if t == cls.LDP_RS_TYPE:
                return LdpRs(uuid)
            else:
                raise ResourceNotExistsError(uuid)


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

        rdfly = cls.load_rdf_layout()
        parent_imr = rdfly.extract_imr(nsc['fcres'][parent_uuid])

        # Set prefix.
        if parent_uuid:
            parent_exists = rdfly.ask_rsrc_exists(parent_imr.identifier)
            if not parent_exists:
                raise ResourceNotExistsError(parent_uuid)

            parent_types = { t.identifier for t in \
                    parent_imr.objects(RDF.type) }
            cls._logger.debug('Parent types: {}'.format(
                    parent_types))
            if nsc['ldp'].Container not in parent_types:
                raise InvalidResourceError('Parent {} is not a container.'
                       .format(parent_uuid))

            pfx = parent_uuid + '/'
        else:
            pfx = ''

        # Create candidate UUID and validate.
        if slug:
            cnd_uuid = pfx + slug
            cnd_rsrc = Resource(rdfly.ds, nsc['fcres'][cnd_uuid])
            if rdfly.ask_rsrc_exists(cnd_rsrc.identifier):
                return cls(pfx + str(uuid4()))
            else:
                return cls(cnd_uuid)
        else:
            return cls(pfx + str(uuid4()))


    ## LDP METHODS ##

    @transactional
    @must_exist
    def delete(self):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_DELETE
        '''
        self.rdfly.delete_rsrc(self.urn)


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
            cparent_uri = self._find_parent_or_create_pairtree(self.uuid)

            # Reroute possible containment relationships between parent and new
            # resource.
            #self._splice_in(cparent)

            if cparent_uri:
                self.rdfly.ds.add((cparent_uri, nsc['ldp'].contains,
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

            if self.rdfly.ask_rsrc_exists(cparent_uri):
                return cparent_uri
            else:
                self._create_path_segment(cparent_uri, cur_child_uri)
                cur_child_uri = cparent_uri

        return None


    def _create_path_segment(self, uri, child_uri):
        '''
        Create a path segment with a non-LDP containment statement.

        This diverges from the default fcrepo4 behavior which creates pairtree
        resources.

        If a resource such as `fcres:a/b/c` is created, and neither fcres:a or
        fcres:a/b exists, we have to create two "hidden" containment statements
        between a and a/b and between a/b and a/b/c in order to maintain the
        `containment chain.
        '''
        g = Graph()
        g.add((uri, RDF.type, nsc['ldp'].Container))
        g.add((uri, RDF.type, nsc['ldp'].BasicContainer))
        g.add((uri, RDF.type, nsc['ldp'].RDFSource))
        g.add((uri, nsc['fcrepo'].contains, child_uri))

        # If the path segment is just below root
        if '/' not in str(uri):
            g.add((nsc['fcsystem'].root, nsc['fcrepo'].contains, uri))

        self.rdfly.create_rsrc(g)



