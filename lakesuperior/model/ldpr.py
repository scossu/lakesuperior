import logging

from abc import ABCMeta
from collections import defaultdict
from importlib import import_module
from itertools import accumulate
from uuid import uuid4

import arrow

from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF, XSD

from lakesuperior.config_parser import config
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import InvalidResourceError, \
        ResourceNotExistsError, ServerManagedTermError
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout
from lakesuperior.util.translator import Translator


def transactional(fn):
    '''
    Decorator for methods of the Ldpr class to handle transactions in an RDF
    store.
    '''
    def wrapper(self, *args, **kwargs):
        try:
            ret = fn(self, *args, **kwargs)
            self._logger.info('Committing transaction.')
            self.rdfly.store.commit()
            return ret
        except:
            self._logger.warn('Rolling back transaction.')
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
    RETURN_CHILD_RES_URI = nsc['fcrepo'].EmbedResources
    RETURN_INBOUND_REF_URI = nsc['fcrepo'].InboundReferences
    RETURN_SRV_MGD_RES_URI = nsc['fcrepo'].ServerManaged

    _logger = logging.getLogger(__name__)

    rdf_store_layout = config['application']['store']['ldp_rs']['layout']
    non_rdf_store_layout = config['application']['store']['ldp_nr']['layout']

    ## MAGIC METHODS ##

    def __init__(self, uuid, retr_opts={}):
        '''Instantiate an in-memory LDP resource that can be loaded from and
        persisted to storage.

        Persistence is done in this class. None of the operations in the store
        layout should commit an open transaction. Methods are wrapped in a
        transaction by using the `@transactional` decorator.

        @param uuid (string) UUID of the resource.
        '''
        self.uuid = uuid

        self._urn = nsc['fcres'][uuid] if self.uuid is not None \
                else BaseRdfLayout.ROOT_NODE_URN

        self._set_imr_options(retr_opts)


    @property
    def urn(self):
        '''
        The internal URI (URN) for the resource as stored in the triplestore.

        This is a URN that needs to be converted to a global URI for the LDP
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
    def rdfly(self):
        '''
        Load RDF store layout.
        '''
        if not hasattr(self, '_rdfly'):
            self._rdfly = __class__.load_layout('rdf')

        return self._rdfly


    @property
    def rsrc(self):
        '''
        The RDFLib resource representing this LDPR. This is a live
        representation of the stored data if present.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_rsrc'):
            self._rsrc = self.rdfly.rsrc(self.urn)

        return self._rsrc


    @property
    def imr(self):
        '''
        Extract an in-memory resource from the graph store.

        If the resource is not stored (yet), a `ResourceNotExistsError` is
        raised.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_imr'):
            self._logger.debug('IMR options: {}'.format(self._imr_options))
            options = dict(self._imr_options, strict=True)
            self._imr = self.rdfly.extract_imr(self.urn, **options)

        return self._imr


    @property
    def stored_or_new_imr(self):
        '''
        Extract an in-memory resource for harmless manipulation and output.

        If the resource is not stored (yet), initialize a new IMR with basic
        triples.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_imr'):
            options = dict(self._imr_options, strict=True)
            try:
                self._imr = self.rdfly.extract_imr(self.urn, **options)
            except ResourceNotExistsError:
                self._imr = Resource(Graph(), self.urn)
                for t in self.base_types:
                    self.imr.add(RDF.type, t)

        return self._imr


    @imr.deleter
    def imr(self):
        '''
        Delete in-memory buffered resource.
        '''
        delattr(self, '_imr')


    @property
    def is_stored(self):
        return self.rdfly.ask_rsrc_exists(self.urn)


    @property
    def types(self):
        '''All RDF types.

        @return set(rdflib.term.URIRef)
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
    def load_layout(cls, type, uuid=None):
        '''
        Dynamically load the store layout indicated in the configuration.

        @param type (string) One of `rdf` or `non_rdf`. Determines the type of
        layout to be loaded.
        @param uuid (string) UUID of the base resource. For RDF layouts only.
        '''
        layout_cls = getattr(cls, '{}_store_layout'.format(type))
        store_mod = import_module('lakesuperior.store_layouts.{0}.{1}'.format(
                type, layout_cls))
        layout_cls = getattr(store_mod, Translator.camelcase(layout_cls))

        return layout_cls()


    @classmethod
    def readonly_inst(cls, uuid, repr_opts=None):
        '''
        Factory method that creates and returns an instance of an LDPR subclass
        based on information that needs to be queried from the underlying
        graph store.

        This is used with retrieval methods for resources that already exist.

        @param uuid UUID of the instance.
        '''
        rdfly = cls.load_layout('rdf')
        imr_urn = nsc['fcres'][uuid] if uuid else rdfly.ROOT_NODE_URN
        imr = rdfly.extract_imr(imr_urn, **repr_opts)
        rdf_types = imr.objects(RDF.type)

        for t in rdf_types:
            cls._logger.debug('Checking RDF type: {}'.format(t.identifier))
            if t.identifier == cls.LDP_NR_TYPE:
                from lakesuperior.model.ldp_nr import LdpNr
                cls._logger.info('Resource is a LDP-NR.')
                return LdpNr(uuid)
            if t.identifier == cls.LDP_RS_TYPE:
                from lakesuperior.model.ldp_rs import LdpRs
                cls._logger.info('Resource is a LDP-RS.')
                return LdpRs(uuid)

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

        rdfly = cls.load_layout('rdf')

        parent_imr_urn = nsc['fcres'][parent_uuid] if parent_uuid \
                else rdfly.ROOT_NODE_URN
        parent_imr = rdfly.extract_imr(parent_imr_urn, minimal=True)
        if not len(parent_imr.graph):
            raise ResourceNotExistsError(parent_uuid)

        # Set prefix.
        if parent_uuid:
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

    def head(self):
        '''
        Return values for the headers.
        '''
        out_headers = defaultdict(list)

        self._logger.debug('IMR options in head(): {}'.format(self._imr_options))
        digest = self.imr.value(nsc['premis'].hasMessageDigest)
        if digest:
            etag = digest.identifier.split(':')[-1]
            out_headers['ETag'] = 'W/"{}"'.format(etag),

        last_updated_term = self.imr.value(nsc['fcrepo'].lastModified)
        if last_updated_term:
            out_headers['Last-Modified'] = arrow.get(last_updated_term)\
                .format('ddd, D MMM YYYY HH:mm:ss Z')

        for t in self.ldp_types:
            out_headers['Link'].append(
                    '{};rel="type"'.format(t.identifier.n3()))

        return out_headers


    def get(self, *args, **kwargs):
        raise NotImplementedError()


    def post(self, *args, **kwargs):
        raise NotImplementedError()


    def put(self, *args, **kwargs):
        raise NotImplementedError()


    def patch(self, *args, **kwargs):
        raise NotImplementedError()


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
        imr = Resource(Graph(), uri)
        imr.add(RDF.type, nsc['ldp'].Container)
        imr.add(RDF.type, nsc['ldp'].BasicContainer)
        imr.add(RDF.type, nsc['ldp'].RDFSource)
        imr.add(nsc['fcrepo'].contains, child_uri)

        # If the path segment is just below root
        if '/' not in str(uri):
            imr.graph.add((nsc['fcsystem'].root, nsc['fcrepo'].contains, uri))

        self.rdfly.create_rsrc(imr)


    def _set_imr_options(self, repr_opts):
        '''
        Set options to retrieve IMR.

        Ideally, IMR retrieval is done once per request, so all the options
        are set once in the `imr()` property.

        @param repr_opts (dict): Options parsed from `Prefer` header.
        '''
        self._imr_options = {}

        minimal = embed_children = incl_inbound = False
        self._imr_options['incl_srv_mgd'] = True

        if 'value' in repr_opts and repr_opts['value'] == 'minimal':
            self._imr_options['minimal'] = True
        elif 'parameters' in repr_opts:
            include = repr_opts['parameters']['include'].split(' ') \
                    if 'include' in repr_opts['parameters'] else []
            omit = repr_opts['parameters']['omit'].split(' ') \
                    if 'omit' in repr_opts['parameters'] else []

            self._logger.debug('Include: {}'.format(include))
            self._logger.debug('Omit: {}'.format(omit))

            if str(self.RETURN_INBOUND_REF_URI) in include:
                    self._imr_options['incl_inbound'] = True
            if str(self.RETURN_CHILD_RES_URI) in omit:
                    self._imr_options['embed_chldren'] = False
            if str(self.RETURN_SRV_MGD_RES_URI) in omit:
                    self._imr_options['incl_srv_mgd'] = False


