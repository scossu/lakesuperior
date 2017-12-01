import logging

from abc import ABCMeta
from collections import defaultdict
from itertools import accumulate
from uuid import uuid4

import arrow

from flask import current_app
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF, XSD
from rdflib.term import URIRef, Literal

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.exceptions import InvalidResourceError, \
        ResourceNotExistsError, ServerManagedTermError
from lakesuperior.store_layouts.ldp_rs.base_rdf_layout import BaseRdfLayout
from lakesuperior.toolbox import Toolbox


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

    EMBED_CHILD_RES_URI = nsc['fcrepo'].EmbedResources
    FCREPO_PTREE_TYPE = nsc['fcrepo'].Pairtree
    INS_CNT_REL_URI = nsc['ldp'].insertedContentRelation
    LDP_NR_TYPE = nsc['ldp'].NonRDFSource
    LDP_RS_TYPE = nsc['ldp'].RDFSource
    MBR_RSRC_URI = nsc['ldp'].membershipResource
    MBR_REL_URI = nsc['ldp'].hasMemberRelation
    RETURN_CHILD_RES_URI = nsc['fcrepo'].Children
    RETURN_INBOUND_REF_URI = nsc['fcrepo'].InboundReferences
    RETURN_SRV_MGD_RES_URI = nsc['fcrepo'].ServerManaged
    ROOT_NODE_URN = nsc['fcsystem'].root

    RES_CREATED = 'Create'
    RES_DELETED = 'Delete'
    RES_UPDATED = 'Update'

    protected_pred = (
        nsc['fcrepo'].created,
        nsc['fcrepo'].createdBy,
        nsc['ldp'].contains,
    )

    _logger = logging.getLogger(__name__)


    ## STATIC & CLASS METHODS ##

    @classmethod
    def inst(cls, uuid, repr_opts=None):
        '''
        Factory method that creates and returns an instance of an LDPR subclass
        based on information that needs to be queried from the underlying
        graph store.

        N.B. The resource must exist.

        @param uuid UUID of the instance.
        '''
        imr_urn = nsc['fcres'][uuid] if uuid else cls.ROOT_NODE_URN
        cls._logger.debug('Representation options: {}'.format(repr_opts))
        imr_opts = cls.set_imr_options(repr_opts)
        imr = current_app.rdfly.extract_imr(imr_urn, **imr_opts)
        rdf_types = set(imr.objects(RDF.type))

        for t in rdf_types:
            cls._logger.debug('Checking RDF type: {}'.format(t.identifier))
            if t.identifier == cls.LDP_NR_TYPE:
                from lakesuperior.model.ldp_nr import LdpNr
                cls._logger.info('Resource is a LDP-NR.')
                return LdpNr(uuid, repr_opts)
            if t.identifier == cls.LDP_RS_TYPE:
                from lakesuperior.model.ldp_rs import LdpRs
                cls._logger.info('Resource is a LDP-RS.')
                return LdpRs(uuid, repr_opts)

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

        parent = cls(parent_uuid, repr_opts={
            'parameters' : {'omit' : cls.RETURN_CHILD_RES_URI}
        })

        # Set prefix.
        if parent_uuid:
            parent_types = { t.identifier for t in \
                    parent.imr.objects(RDF.type) }
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
            cnd_rsrc = Resource(current_app.rdfly.ds, nsc['fcres'][cnd_uuid])
            if current_app.rdfly.ask_rsrc_exists(cnd_rsrc.identifier):
                return cls(pfx + str(uuid4()))
            else:
                return cls(cnd_uuid)
        else:
            return cls(pfx + str(uuid4()))


    @classmethod
    def set_imr_options(cls, repr_opts):
        '''
        Set options to retrieve IMR.

        Ideally, IMR retrieval is done once per request, so all the options
        are set once in the `imr()` property.

        @param repr_opts (dict): Options parsed from `Prefer` header.
        '''
        cls._logger.debug('Setting retrieval options from: {}'.format(repr_opts))
        imr_options = {}

        if repr_opts.setdefault('value') == 'minimal':
            imr_options = {
                'embed_children' : False,
                'incl_children' : False,
                'incl_inbound' : False,
                'incl_srv_mgd' : False,
            }
        else:
            # Default.
            imr_options = {
                'embed_children' : False,
                'incl_children' : True,
                'incl_inbound' : False,
                'incl_srv_mgd' : True,
            }

            # Override defaults.
            if 'parameters' in repr_opts:
                include = repr_opts['parameters']['include'].split(' ') \
                        if 'include' in repr_opts['parameters'] else []
                omit = repr_opts['parameters']['omit'].split(' ') \
                        if 'omit' in repr_opts['parameters'] else []

                cls._logger.debug('Include: {}'.format(include))
                cls._logger.debug('Omit: {}'.format(omit))

                if str(cls.EMBED_CHILD_RES_URI) in include:
                        imr_options['embed_children'] = True
                if str(cls.RETURN_CHILD_RES_URI) in omit:
                        imr_options['incl_children'] = False
                if str(cls.RETURN_INBOUND_REF_URI) in include:
                        imr_options['incl_inbound'] = True
                if str(cls.RETURN_SRV_MGD_RES_URI) in omit:
                        imr_options['incl_srv_mgd'] = False

        cls._logger.debug('Retrieval options: {}'.format(imr_options))

        return imr_options


    ## MAGIC METHODS ##

    def __init__(self, uuid, repr_opts={}):
        '''Instantiate an in-memory LDP resource that can be loaded from and
        persisted to storage.

        Persistence is done in this class. None of the operations in the store
        layout should commit an open transaction. Methods are wrapped in a
        transaction by using the `@transactional` decorator.

        @param uuid (string) UUID of the resource. If None (must be explicitly
        set) it refers to the root node.
        '''
        self.uuid = uuid
        self.urn = nsc['fcres'][uuid] if self.uuid else self.ROOT_NODE_URN
        self.uri = Toolbox().uuid_to_uri(self.uuid)

        self.repr_opts = repr_opts
        self._imr_options = __class__.set_imr_options(self.repr_opts)

        self.rdfly = current_app.rdfly
        self.nonrdfly = current_app.nonrdfly


    @property
    def rsrc(self):
        '''
        The RDFLib resource representing this LDPR. This is a live
        representation of the stored data if present.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_rsrc'):
            self._rsrc = self.rdfly.ds.resource(self.urn)

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
    def out_graph(self):
        '''
        Retun a globalized graph of the resource's IMR.

        Internal URNs are replaced by global URIs using the endpoint webroot.
        '''
        # Remove digest hash.
        self.imr.remove(nsc['premis'].hasMessageDigest)

        if not self._imr_options.setdefault('incl_srv_mgd', False):
            for p in srv_mgd_predicates:
                self._logger.debug('Removing predicate: {}'.format(p))
                self.imr.remove(p)
            for t in srv_mgd_types:
                self._logger.debug('Removing type: {}'.format(t))
                self.imr.remove(RDF.type, t)

        out_g = Toolbox().globalize_graph(self.imr.graph)
        # Clear IMR because it's been pruned. In the rare case it is needed
        # after this method, it will be retrieved again.
        delattr(self, 'imr')

        return out_g


    @property
    def is_stored(self):
        return self.rdfly.ask_rsrc_exists(self.urn)


    @property
    def types(self):
        '''All RDF types.

        @return set(rdflib.term.URIRef)
        '''
        if not hasattr(self, '_types'):
            self._types = self.imr.graph[self.imr.identifier : RDF.type]

        return self._types


    @property
    def ldp_types(self):
        '''The LDP types.

        @return set(rdflib.term.URIRef)
        '''
        if not hasattr(self, '_ldp_types'):
            self._ldp_types = { t for t in self.types if t[:4] == 'ldp:' }

        return self._ldp_types


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
                    '{};rel="type"'.format(t.n3()))

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
    def delete(self, inbound=True, delete_children=True, leave_tstone=True):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_DELETE

        @param inbound (boolean) If specified, delete all inbound relationships
        as well. This is the default and is always the case if referential
        integrity is enforced by configuration.
        @param delete_children (boolean) Whether to delete all child resources.
        This is the default.
        '''
        refint = current_app.config['store']['ldp_rs']['referential_integrity']
        inbound = True if refint else inbound

        children = self.imr[nsc['ldp'].contains * '+'] \
                if delete_children else []

        ret = self._delete_rsrc(inbound, leave_tstone)

        for child_uri in children:
            child_rsrc = Ldpr.inst(
                Toolbox().uri_to_uuid(child_uri.identifier), self.repr_opts)
            child_rsrc._delete_rsrc(inbound, leave_tstone,
                    tstone_pointer=self.urn)

        return ret


    @transactional
    def delete_tombstone(self):
        '''
        Delete a tombstone.

        N.B. This does not trigger an event.
        '''
        remove_trp = {
            (self.urn, RDF.type, nsc['fcsystem'].Tombstone),
            (self.urn, nsc['fcrepo'].created, None),
            (None, nsc['fcsystem'].tombstone, self.urn),
        }
        self.rdfly.modify_dataset(remove_trp)


    ## PROTECTED METHODS ##

    def _create_rsrc(self):
        '''
        Create a new resource by comparing an empty graph with the provided
        IMR graph.
        '''
        self.rdfly.modify_dataset(add_trp=self.provided_imr.graph)

        return self.RES_CREATED


    def _replace_rsrc(self):
        '''
        Replace a resource.

        The existing resource graph is removed except for the protected terms.
        '''
        # The extracted IMR is used as a "minus" delta, so protected predicates
        # must be removed.
        for p in self.protected_pred:
            self.imr.remove(p)

        delta = self._dedup_deltas(self.imr.graph, self.provided_imr.graph)
        self.rdfly.modify_dataset(*delta)

        # Reset the IMR because it has changed.
        delattr(self, 'imr')

        return self.RES_UPDATED


    def _delete_rsrc(self, inbound, leave_tstone=True, tstone_pointer=None):
        '''
        Delete a single resource and create a tombstone.

        @param inbound (boolean) Whether to delete the inbound relationships.
        @param tstone_pointer (URIRef) If set to a URN, this creates a pointer
        to the tombstone of the resource that used to contain the deleted
        resource. Otherwise the delete resource becomes a tombstone.
        '''
        self._logger.info('Removing resource {}'.format(self.urn))

        remove_trp = set(self.imr.graph)
        add_trp = set()

        if leave_tstone:
            if tstone_pointer:
                add_trp.add((self.urn, nsc['fcsystem'].tombstone,
                        tstone_pointer))
            else:
                ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)
                add_trp.add((self.urn, RDF.type, nsc['fcsystem'].Tombstone))
                add_trp.add((self.urn, nsc['fcrepo'].created, ts))
        else:
            self._logger.info('NOT leaving tombstone.')

        if inbound:
            for ib_rsrc_uri in self.imr.graph.subjects(None, self.urn):
                remove_trp.add((ib_rsrc_uri, None, self.urn))

        self.rdfly.modify_dataset(remove_trp, add_trp)

        return self.RES_DELETED


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
            parent_uri = self._find_parent_or_create_pairtree(self.uuid)

            if parent_uri:
                self.rdfly.ds.add((parent_uri, nsc['ldp'].contains,
                        self.rsrc.identifier))

                # Direct or indirect container relationship.
                self._add_ldp_dc_ic_rel(parent_uri)
        else:
            self.rsrc.graph.add((nsc['fcsystem'].root, nsc['ldp'].contains,
                    self.rsrc.identifier))


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


    def _dedup_deltas(self, remove_g, add_g):
        '''
        Remove duplicate triples from add and remove delta graphs, which would
        otherwise contain unnecessary statements that annul each other.
        '''
        return (
            remove_g - add_g,
            add_g - remove_g
        )


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


    def _add_ldp_dc_ic_rel(self, cont_uri):
        '''
        Add relationship triples from a direct or indirect container parent.

        @param cont_uri (rdflib.term.URIRef)  The container URI.
        '''
        cont_imr = self.rdfly.extract_imr(cont_uri, incl_children=False)
        cont_p = set(cont_imr.graph.predicates())
        add_g = Graph()

        self._logger.info('Checking direct or indirect containment.')
        self._logger.debug('Parent predicates: {}'.format(cont_p))

        if self.MBR_RSRC_URI in cont_p and self.MBR_REL_URI in cont_p:
            s = Toolbox().localize_term(
                    cont_imr.value(self.MBR_RSRC_URI).identifier)
            p = cont_imr.value(self.MBR_REL_URI).identifier

            if cont_imr[RDF.type : nsc['ldp'].DirectContainer]:
                self._logger.info('Parent is a direct container.')

                self._logger.debug('Creating DC triples.')
                add_g.add((s, p, self.urn))

            elif cont_imr[RDF.type : nsc['ldp'].IndirectContainer] \
                   and self.INS_CNT_REL_URI in cont_p:
                self._logger.info('Parent is an indirect container.')
                cont_rel_uri = cont_imr.value(self.INS_CNT_REL_URI).identifier
                target_uri = self.provided_imr.value(cont_rel_uri).identifier
                self._logger.debug('Target URI: {}'.format(target_uri))
                if target_uri:
                    self._logger.debug('Creating IC triples.')
                    add_g.add((s, p, target_uri))

        if len(add_g):
            add_g = self._check_mgd_terms(add_g)
            self._logger.debug('Adding DC/IC triples: {}'.format(
                add_g.serialize(format='turtle').decode('utf-8')))
            self.rdfly.modify_dataset(Graph(), add_g)


