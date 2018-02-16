import logging

from abc import ABCMeta
from collections import defaultdict
from itertools import accumulate, groupby
#from pprint import pformat
from uuid import uuid4

import arrow

from flask import current_app, g
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF
from rdflib.term import URIRef, Literal

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.exceptions import (RefIntViolationError,
        ResourceNotExistsError, ServerManagedTermError, TombstoneError)
from lakesuperior.model.ldp_factory import LdpFactory
from lakesuperior.store_layouts.ldp_rs.rsrc_centric_layout import (
        VERS_CONT_LABEL)


ROOT_UID = ''
ROOT_RSRC_URI = nsc['fcres'][ROOT_UID]


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
    MBR_RSRC_URI = nsc['ldp'].membershipResource
    MBR_REL_URI = nsc['ldp'].hasMemberRelation
    RETURN_CHILD_RES_URI = nsc['fcrepo'].Children
    RETURN_INBOUND_REF_URI = nsc['fcrepo'].InboundReferences
    RETURN_SRV_MGD_RES_URI = nsc['fcrepo'].ServerManaged

    # Workflow type. Inbound means that the resource is being written to the
    # store, outbounnd is being retrieved for output.
    WRKF_INBOUND = '_workflow:inbound_'
    WRKF_OUTBOUND = '_workflow:outbound_'

    # Default user to be used for the `createdBy` and `lastUpdatedBy` if a user
    # is not provided.
    DEFAULT_USER = Literal('BypassAdmin')

    RES_CREATED = '_create_'
    RES_DELETED = '_delete_'
    RES_UPDATED = '_update_'

    # RDF Types that populate a new resource.
    base_types = {
        nsc['fcrepo'].Resource,
        nsc['ldp'].Resource,
        nsc['ldp'].RDFSource,
    }

    # Predicates that do not get removed when a resource is replaced.
    protected_pred = (
        nsc['fcrepo'].created,
        nsc['fcrepo'].createdBy,
        nsc['ldp'].contains,
    )

    # Server-managed RDF types ignored in the RDF payload if the resource is
    # being created. N.B. These still raise an error if the resource exists.
    smt_allow_on_create = {
        nsc['ldp'].DirectContainer,
        nsc['ldp'].IndirectContainer,
    }

    _logger = logging.getLogger(__name__)


    ## MAGIC METHODS ##

    def __init__(self, uid, repr_opts={}, provided_imr=None, **kwargs):
        '''Instantiate an in-memory LDP resource that can be loaded from and
        persisted to storage.

        @param uid (string) uid of the resource. If None (must be explicitly
        set) it refers to the root node. It can also be the full URI or URN,
        in which case it will be converted.
        @param repr_opts (dict) Options used to retrieve the IMR. See
        `parse_rfc7240` for format details.
        @Param provd_rdf (string) RDF data provided by the client in
        operations such as `PUT` or `POST`, serialized as a string. This sets
        the `provided_imr` property.
        '''
        self.uid = g.tbox.uri_to_uuid(uid) \
                if isinstance(uid, URIRef) else uid
        self.urn = nsc['fcres'][uid]
        self.uri = g.tbox.uuid_to_uri(self.uid)

        self.rdfly = current_app.rdfly
        self.nonrdfly = current_app.nonrdfly

        self.provided_imr = provided_imr


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
            if hasattr(self, '_imr_options'):
                self._logger.info('Getting RDF representation for resource /{}'
                        .format(self.uid))
                #self._logger.debug('IMR options: {}'.format(self._imr_options))
                imr_options = self._imr_options
            else:
                imr_options = {}
            options = dict(imr_options, strict=True)
            self._imr = self.rdfly.extract_imr(self.uid, **options)

        return self._imr


    @imr.setter
    def imr(self, v):
        '''
        Replace in-memory buffered resource.

        @param v (set | rdflib.Graph) New set of triples to populate the IMR
        with.
        '''
        if isinstance(v, Resource):
            v = v.graph
        self._imr = Resource(Graph(), self.urn)
        gr = self._imr.graph
        gr += v


    @imr.deleter
    def imr(self):
        '''
        Delete in-memory buffered resource.
        '''
        delattr(self, '_imr')


    @property
    def metadata(self):
        '''
        Get resource metadata.
        '''
        if not hasattr(self, '_metadata'):
            if hasattr(self, '_imr'):
                self._logger.info('Metadata is IMR.')
                self._metadata = self._imr
            else:
                self._logger.info('Getting metadata for resource /{}'
                        .format(self.uid))
                self._metadata = self.rdfly.get_metadata(self.uid)

        return self._metadata


    @metadata.setter
    def metadata(self, rsrc):
        '''
        Set resource metadata.
        '''
        if not isinstance(rsrc, Resource):
            raise TypeError('Provided metadata is not a Resource object.')
        self._metadata = rsrc


    @property
    def stored_or_new_imr(self):
        '''
        Extract an in-memory resource for harmless manipulation and output.

        If the resource is not stored (yet), initialize a new IMR with basic
        triples.

        @return rdflib.resource.Resource
        '''
        if not hasattr(self, '_imr'):
            if hasattr(self, '_imr_options'):
                #self._logger.debug('IMR options: {}'.format(self._imr_options))
                imr_options = self._imr_options
            else:
                imr_options = {}
            options = dict(imr_options, strict=True)
            try:
                self._imr = self.rdfly.extract_imr(self.uid, **options)
            except ResourceNotExistsError:
                self._imr = Resource(Graph(), self.urn)
                for t in self.base_types:
                    self.imr.add(RDF.type, t)

        return self._imr


    @property
    def out_graph(self):
        '''
        Retun a graph of the resource's IMR formatted for output.
        '''
        out_gr = Graph()

        for t in self.imr.graph:
            if (
                # Exclude digest hash and version information.
                t[1] not in {
                    nsc['premis'].hasMessageDigest,
                    nsc['fcrepo'].hasVersion,
                }
            ) and (
                # Only include server managed triples if requested.
                self._imr_options.get('incl_srv_mgd', True)
                or not self._is_trp_managed(t)
            ):
                out_gr.add(t)

        return out_gr


    @property
    def version_info(self):
        '''
        Return version metadata (`fcr:versions`).
        '''
        if not hasattr(self, '_version_info'):
            try:
                self._version_info = self.rdfly.get_version_info(self.uid)
            except ResourceNotExistsError as e:
                self._version_info = Resource(Graph(), self.urn)

        return self._version_info


    @property
    def version_uids(self):
        '''
        Return a generator of version UIDs (relative to their parent resource).
        '''
        gen = self.version_info[
                nsc['fcrepo'].hasVersion / nsc['fcrepo'].hasVersionLabel]

        return { str(uid) for uid in gen }


    @property
    def is_stored(self):
        if not hasattr(self, '_is_stored'):
            if hasattr(self, '_imr'):
                self._is_stored = len(self.imr.graph) > 0
            else:
                self._is_stored = self.rdfly.ask_rsrc_exists(self.uid)

        return self._is_stored


    @property
    def types(self):
        '''All RDF types.

        @return set(rdflib.term.URIRef)
        '''
        if not hasattr(self, '_types'):
            if len(self.metadata.graph):
                metadata = self.metadata
            elif getattr(self, 'provided_imr', None) and \
                    len(self.provided_imr.graph):
                metadata = self.provided_imr
            else:
                return set()

            self._types = set(metadata.graph[self.urn : RDF.type])

        return self._types


    @property
    def ldp_types(self):
        '''The LDP types.

        @return set(rdflib.term.URIRef)
        '''
        if not hasattr(self, '_ldp_types'):
            self._ldp_types = { t for t in self.types if nsc['ldp'] in t }

        return self._ldp_types


    ## LDP METHODS ##

    def head(self):
        '''
        Return values for the headers.
        '''
        out_headers = defaultdict(list)

        digest = self.metadata.value(nsc['premis'].hasMessageDigest)
        if digest:
            etag = digest.identifier.split(':')[-1]
            out_headers['ETag'] = 'W/"{}"'.format(etag),

        last_updated_term = self.metadata.value(nsc['fcrepo'].lastModified)
        if last_updated_term:
            out_headers['Last-Modified'] = arrow.get(last_updated_term)\
                .format('ddd, D MMM YYYY HH:mm:ss Z')

        for t in self.ldp_types:
            out_headers['Link'].append(
                    '{};rel="type"'.format(t.n3()))

        return out_headers



    def get(self):
        '''
        Get an RDF representation of the resource.

        The binary retrieval is handled directly by the router.

        Internal URNs are replaced by global URIs using the endpoint webroot.
        '''
        gr = g.tbox.globalize_graph(self.out_graph)
        gr.namespace_manager = nsm

        return gr


    def get_version_info(self):
        '''
        Get the `fcr:versions` graph.
        '''
        gr = g.tbox.globalize_graph(self.version_info.graph)
        gr.namespace_manager = nsm

        return gr


    def get_version(self, ver_uid, **kwargs):
        '''
        Get a version by label.
        '''
        ver_gr = self.rdfly.extract_imr(self.uid, ver_uid, **kwargs).graph

        gr = g.tbox.globalize_graph(ver_gr)
        gr.namespace_manager = nsm

        return gr


    def post(self):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

        Perform a POST action after a valid resource URI has been found.
        '''
        return self._create_or_replace_rsrc(create_only=True)


    def put(self):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT
        '''
        return self._create_or_replace_rsrc()


    def patch(self, *args, **kwargs):
        raise NotImplementedError()


    def delete(self, inbound=True, delete_children=True, leave_tstone=True):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_DELETE

        @param inbound (boolean) If specified, delete all inbound relationships
        as well. This is the default and is always the case if referential
        integrity is enforced by configuration.
        @param delete_children (boolean) Whether to delete all child resources.
        This is the default.
        '''
        refint = self.rdfly.config['referential_integrity']
        inbound = True if refint else inbound

        children = (self.rdfly.get_recursive(self.uid, nsc['ldp'].contains)
                if delete_children else [])

        if leave_tstone:
            ret = self._bury_rsrc(inbound)
        else:
            ret = self._purge_rsrc(inbound)

        for child_uri in children:
            try:
                child_rsrc = LdpFactory.from_stored(
                    g.tbox.uri_to_uuid(child_uri.identifier),
                    repr_opts={'incl_children' : False})
            except (TombstoneError, ResourceNotExistsError):
                continue
            if leave_tstone:
                child_rsrc._bury_rsrc(inbound, tstone_pointer=self.urn)
            else:
                child_rsrc._purge_rsrc(inbound)

        return ret


    def resurrect(self):
        '''
        Resurrect a resource from a tombstone.

        @EXPERIMENTAL
        '''
        tstone_trp = set(self.rdfly.extract_imr(self.uid, strict=False).graph)

        ver_rsp = self.version_info.graph.query('''
        SELECT ?uid {
          ?latest fcrepo:hasVersionLabel ?uid ;
            fcrepo:created ?ts .
        }
        ORDER BY DESC(?ts)
        LIMIT 1
        ''')
        ver_uid = str(ver_rsp.bindings[0]['uid'])
        ver_trp = set(self.rdfly.get_metadata(self.uid, ver_uid).graph)

        laz_gr = Graph()
        for t in ver_trp:
            if t[1] != RDF.type or t[2] not in {
                nsc['fcrepo'].Version,
            }:
                laz_gr.add((self.urn, t[1], t[2]))
        laz_gr.add((self.urn, RDF.type, nsc['fcrepo'].Resource))
        if nsc['ldp'].NonRdfSource in laz_gr[: RDF.type :]:
            laz_gr.add((self.urn, RDF.type, nsc['fcrepo'].Binary))
        elif nsc['ldp'].Container in laz_gr[: RDF.type :]:
            laz_gr.add((self.urn, RDF.type, nsc['fcrepo'].Container))

        self._modify_rsrc(self.RES_CREATED, tstone_trp, set(laz_gr))
        self._set_containment_rel()

        return self.uri



    def purge(self, inbound=True):
        '''
        Delete a tombstone and all historic snapstots.

        N.B. This does not trigger an event.
        '''
        refint = current_app.config['store']['ldp_rs']['referential_integrity']
        inbound = True if refint else inbound

        return self._purge_rsrc(inbound)


    def create_version(self, ver_uid=None):
        '''
        Create a new version of the resource.

        NOTE: This creates an event only for the resource being updated (due
        to the added `hasVersion` triple and possibly to the `hasVersions` one)
        but not for the version being created.

        @param ver_uid Version ver_uid. If already existing, an exception is
        raised.
        '''
        if not ver_uid or ver_uid in self.version_uids:
            ver_uid = str(uuid4())

        return g.tbox.globalize_term(self.create_rsrc_snapshot(ver_uid))


    def revert_to_version(self, ver_uid, backup=True):
        '''
        Revert to a previous version.

        @param ver_uid (string) Version UID.
        @param backup (boolean) Whether to create a backup snapshot. Default is
        true.
        '''
        # Create a backup snapshot.
        if backup:
            self.create_version()

        ver_gr = self.rdfly.extract_imr(self.uid, ver_uid=ver_uid,
                incl_children=False)
        self.provided_imr = Resource(Graph(), self.urn)

        for t in ver_gr.graph:
            if not self._is_trp_managed(t):
                self.provided_imr.add(t[1], t[2])
            # @TODO Check individual objects: if they are repo-managed URIs
            # and not existing or tombstones, they are not added.

        return self._create_or_replace_rsrc(create_only=False)


    ## PROTECTED METHODS ##

    def _is_trp_managed(self, t):
        '''
        Whether a triple is server-managed.

        @return boolean
        '''
        return t[1] in srv_mgd_predicates or (
                t[1] == RDF.type and t[2] in srv_mgd_types)


    def _create_or_replace_rsrc(self, create_only=False):
        '''
        Create or update a resource. PUT and POST methods, which are almost
        identical, are wrappers for this method.

        @param create_only (boolean) Whether this is a create-only operation.
        '''
        create = create_only or not self.is_stored

        self._add_srv_mgd_triples(create)
        #self._ensure_single_subject_rdf(self.provided_imr.graph)
        ref_int = self.rdfly.config['referential_integrity']
        if ref_int:
            self._check_ref_int(ref_int)

        self.rdfly.create_or_replace_rsrc(self.uid, self.provided_imr.graph)
        self.imr = self.provided_imr

        self._set_containment_rel()

        return self.RES_CREATED if create else self.RES_UPDATED
        #return self._head(self.provided_imr.graph)


    def _bury_rsrc(self, inbound, tstone_pointer=None):
        '''
        Delete a single resource and create a tombstone.

        @param inbound (boolean) Whether to delete the inbound relationships.
        @param tstone_pointer (URIRef) If set to a URN, this creates a pointer
        to the tombstone of the resource that used to contain the deleted
        resource. Otherwise the deleted resource becomes a tombstone.
        '''
        self._logger.info('Burying resource {}'.format(self.uid))
        # Create a backup snapshot for resurrection purposes.
        self.create_rsrc_snapshot(uuid4())

        remove_trp = set(self.imr.graph)

        if tstone_pointer:
            add_trp = {(self.urn, nsc['fcsystem'].tombstone,
                    tstone_pointer)}
        else:
            add_trp = {
                (self.urn, RDF.type, nsc['fcsystem'].Tombstone),
                (self.urn, nsc['fcrepo'].created, g.timestamp_term),
            }

        self._modify_rsrc(self.RES_DELETED, remove_trp, add_trp)

        if inbound:
            for ib_rsrc_uri in self.imr.graph.subjects(None, self.urn):
                remove_trp = {(ib_rsrc_uri, None, self.urn)}
                ib_rsrc = Ldpr(ib_rsrc_uri)
                # To preserve inbound links in history, create a snapshot
                ib_rsrc.create_rsrc_snapshot(uuid4())
                ib_rsrc._modify_rsrc(self.RES_UPDATED, remove_trp)

        return self.RES_DELETED


    def _purge_rsrc(self, inbound):
        '''
        Remove all traces of a resource and versions.
        '''
        self._logger.info('Purging resource {}'.format(self.uid))
        self.rdfly.purge_rsrc(self.uid, inbound)

        # @TODO This could be a different event type.
        return self.RES_DELETED


    def create_rsrc_snapshot(self, ver_uid):
        '''
        Perform version creation and return the internal URN.
        '''
        # Create version resource from copying the current state.
        self._logger.info(
                'Creating version snapshot {} for resource {}.'.format(
                    ver_uid, self.uid))
        ver_add_gr = set()
        vers_uid = '{}/{}'.format(self.uid, VERS_CONT_LABEL)
        ver_uid = '{}/{}'.format(vers_uid, ver_uid)
        ver_uri = nsc['fcres'][ver_uid]
        ver_add_gr.add((ver_uri, RDF.type, nsc['fcrepo'].Version))
        for t in self.imr.graph:
            if (
                t[1] == RDF.type and t[2] in {
                    nsc['fcrepo'].Binary,
                    nsc['fcrepo'].Container,
                    nsc['fcrepo'].Resource,
                }
            ) or (
                t[1] in {
                    nsc['fcrepo'].hasParent,
                    nsc['fcrepo'].hasVersions,
                    nsc['fcrepo'].hasVersion,
                    nsc['premis'].hasMessageDigest,
                }
            ):
                pass
            else:
                ver_add_gr.add((
                        g.tbox.replace_term_domain(t[0], self.urn, ver_uri),
                        t[1], t[2]))

        self.rdfly.modify_rsrc(ver_uid, add_trp=ver_add_gr)

        # Update resource admin data.
        rsrc_add_gr = {
            (self.urn, nsc['fcrepo'].hasVersion, ver_uri),
            (self.urn, nsc['fcrepo'].hasVersions, nsc['fcres'][vers_uid]),
        }
        self._modify_rsrc(self.RES_UPDATED, add_trp=rsrc_add_gr, notify=False)

        return nsc['fcres'][ver_uid]


    def _modify_rsrc(self, ev_type, remove_trp=set(), add_trp=set(),
             notify=True):
        '''
        Low-level method to modify a graph for a single resource.

        This is a crucial point for messaging. Any write operation on the RDF
        store that needs to be notified should be performed by invoking this
        method.

        @param ev_type (string) The type of event (create, update, delete).
        @param remove_trp (set) Triples to be removed.
        @param add_trp (set) Triples to be added.
        @param notify (boolean) Whether to send a message about the change.
        '''
        #for trp in [remove_trp, add_trp]:
        #    if not isinstance(trp, set):
        #        trp = set(trp)

        ret = self.rdfly.modify_rsrc(self.uid, remove_trp, add_trp)

        #if notify and current_app.config.get('messaging'):
        #    self._send_msg(ev_type, remove_trp, add_trp)

        return ret


    def _send_msg(self, ev_type, remove_trp=None, add_trp=None):
        '''
        Sent a message about a changed (created, modified, deleted) resource.
        '''
        try:
            type = self.types
            actor = self.metadata.value(nsc['fcrepo'].createdBy)
        except (ResourceNotExistsError, TombstoneError):
            type = set()
            actor = None
            for t in add_trp:
                if t[1] == RDF.type:
                    type.add(t[2])
                elif actor is None and t[1] == nsc['fcrepo'].createdBy:
                    actor = t[2]

        g.changelog.append((set(remove_trp), set(add_trp), {
            'ev_type' : ev_type,
            'time' : g.timestamp,
            'type' : type,
            'actor' : actor,
        }))


    # Not used. @TODO Deprecate or reimplement depending on requirements.
    #def _ensure_single_subject_rdf(self, gr, add_fragment=True):
    #    '''
    #    Ensure that a RDF payload for a POST or PUT has a single resource.
    #    '''
    #    for s in set(gr.subjects()):
    #        # Fragment components
    #        if '#' in s:
    #            parts = s.split('#')
    #            frag = s
    #            s = URIRef(parts[0])
    #            if add_fragment:
    #                # @TODO This is added to the main graph. It should be added
    #                # to the metadata graph.
    #                gr.add((frag, nsc['fcsystem'].fragmentOf, s))
    #        if not s == self.urn:
    #            raise SingleSubjectError(s, self.uid)


    def _check_ref_int(self, config):
        gr = self.provided_imr.graph

        for o in gr.objects():
            if isinstance(o, URIRef) and str(o).startswith(g.webroot)\
                    and not self.rdfly.ask_rsrc_exists(o):
                if config == 'strict':
                    raise RefIntViolationError(o)
                else:
                    self._logger.info(
                            'Removing link to non-existent repo resource: {}'
                            .format(o))
                    gr.remove((None, None, o))


    def _check_mgd_terms(self, gr):
        '''
        Check whether server-managed terms are in a RDF payload.

        @param gr (rdflib.Graph) The graph to validate.
        '''
        offending_subjects = set(gr.subjects()) & srv_mgd_subjects
        if offending_subjects:
            if self.handling=='strict':
                raise ServerManagedTermError(offending_subjects, 's')
            else:
                for s in offending_subjects:
                    self._logger.info('Removing offending subj: {}'.format(s))
                    gr.remove((s, None, None))

        offending_predicates = set(gr.predicates()) & srv_mgd_predicates
        # Allow some predicates if the resource is being created.
        if offending_predicates:
            if self.handling=='strict':
                raise ServerManagedTermError(offending_predicates, 'p')
            else:
                for p in offending_predicates:
                    self._logger.info('Removing offending pred: {}'.format(p))
                    gr.remove((None, p, None))

        offending_types = set(gr.objects(predicate=RDF.type)) & srv_mgd_types
        if not self.is_stored:
            offending_types -= self.smt_allow_on_create
        if offending_types:
            if self.handling=='strict':
                raise ServerManagedTermError(offending_types, 't')
            else:
                for t in offending_types:
                    self._logger.info('Removing offending type: {}'.format(t))
                    gr.remove((None, RDF.type, t))

        #self._logger.debug('Sanitized graph: {}'.format(gr.serialize(
        #    format='turtle').decode('utf-8')))
        return gr


    def _add_srv_mgd_triples(self, create=False):
        '''
        Add server-managed triples to a provided IMR.

        @param create (boolean) Whether the resource is being created.
        '''
        # Base LDP types.
        for t in self.base_types:
            self.provided_imr.add(RDF.type, t)

        # Message digest.
        cksum = g.tbox.rdf_cksum(self.provided_imr.graph)
        self.provided_imr.set(nsc['premis'].hasMessageDigest,
                URIRef('urn:sha1:{}'.format(cksum)))

        # Create and modify timestamp.
        if create:
            self.provided_imr.set(nsc['fcrepo'].created, g.timestamp_term)
            self.provided_imr.set(nsc['fcrepo'].createdBy, self.DEFAULT_USER)
        else:
            self.provided_imr.set(nsc['fcrepo'].created, self.metadata.value(
                    nsc['fcrepo'].created))
            self.provided_imr.set(nsc['fcrepo'].createdBy, self.metadata.value(
                    nsc['fcrepo'].createdBy))

        self.provided_imr.set(nsc['fcrepo'].lastModified, g.timestamp_term)
        self.provided_imr.set(nsc['fcrepo'].lastModifiedBy, self.DEFAULT_USER)


    def _set_containment_rel(self):
        '''Find the closest parent in the path indicated by the uid and
        establish a containment triple.

        E.g. if only urn:fcres:a (short: a) exists:
        - If a/b/c/d is being created, a becomes container of a/b/c/d. Also,
          pairtree nodes are created for a/b and a/b/c.
        - If e is being created, the root node becomes container of e.
        '''
        if '/' in self.uid:
            # Traverse up the hierarchy to find the parent.
            parent_uid = self._find_parent_or_create_pairtree()
        else:
            parent_uid = ROOT_UID

        add_gr = Graph()
        add_gr.add((nsc['fcres'][parent_uid], nsc['ldp'].contains, self.urn))
        parent_rsrc = LdpFactory.from_stored(
                parent_uid, repr_opts={
                'incl_children' : False}, handling='none')
        parent_rsrc._modify_rsrc(self.RES_UPDATED, add_trp=add_gr)

        # Direct or indirect container relationship.
        self._add_ldp_dc_ic_rel(parent_rsrc)


    def _find_parent_or_create_pairtree(self):
        '''
        Check the path-wise parent of the new resource. If it exists, return
        its UID. Otherwise, create pairtree resources up the path until an
        actual resource or the root node is found.

        @return string Resource UID.
        '''
        path_components = self.uid.split('/')

         # If there is only one element, the parent is the root node.
        if len(path_components) < 2:
            return ROOT_UID

        # Build search list, e.g. for a/b/c/d/e would be a/b/c/d, a/b/c, a/b, a
        self._logger.info('Path components: {}'.format(path_components))
        fwd_search_order = accumulate(
            list(path_components)[:-1],
            func=lambda x,y : x + '/' + y
        )
        rev_search_order = reversed(list(fwd_search_order))

        cur_child_uid = self.uid
        parent_uid = ROOT_UID # Defaults to root
        segments = []
        for cparent_uid in rev_search_order:
            if self.rdfly.ask_rsrc_exists(cparent_uid):
                # If a real parent is found, set that and break the loop.
                parent_uid = cparent_uid
                break
            else:
                # Otherwise, add to the list of segments to be built.
                segments.append((cparent_uid, cur_child_uid))
                cur_child_uid = cparent_uid

        for segm_uid, next_uid in segments:
            self.rdfly.add_path_segment(uid=segm_uid, next_uid=next_uid,
                    child_uid=self.uid, parent_uid=parent_uid)

        return parent_uid


    def _dedup_deltas(self, remove_gr, add_gr):
        '''
        Remove duplicate triples from add and remove delta graphs, which would
        otherwise contain unnecessary statements that annul each other.
        '''
        return (
            remove_gr - add_gr,
            add_gr - remove_gr
        )


    #def _create_path_segment(self, uid, child_uid, parent_uid):
    #    '''
    #    Create a path segment with a non-LDP containment statement.

    #    If a resource such as `fcres:a/b/c` is created, and neither fcres:a or
    #    fcres:a/b exists, we have to create two "hidden" containment statements
    #    between a and a/b and between a/b and a/b/c in order to maintain the
    #    containment chain.

    #    These triples are stored separately and are not versioned.
    #    '''
    #    rsrc_uri = nsc['fcres'][uid]

    #    add_trp = {
    #        (rsrc_uri, nsc['fcsystem'].contains, nsc['fcres'][child_uid]),
    #        (rsrc_uri, nsc['ldp'].contains, self.urn),
    #        (rsrc_uri, RDF.type, nsc['ldp'].Container),
    #        (rsrc_uri, RDF.type, nsc['ldp'].BasicContainer),
    #        (rsrc_uri, RDF.type, nsc['ldp'].RDFSource),
    #        (rsrc_uri, RDF.type, nsc['fcrepo'].Pairtree),
    #        (rsrc_uri, nsc['fcrepo'].hasParent, nsc['fcres'][real_parent_uid]),
    #    }

    #    self.rdfly.add_segment(nsc['fcres'][uid], next=self.urn,
    #            child=nsc['fcres'][child_uid],
    #            parent=nsc['fcres'][parent_uid])

    #    # If the path segment is just below root
    #    if '/' not in uid:
    #        self.rdfly.modify_rsrc(ROOT_UID, add_trp={
    #            (ROOT_RSRC_URI, nsc['fcsystem'].contains, nsc['fcres'][uid])
    #        })


    def _add_ldp_dc_ic_rel(self, cont_rsrc):
        '''
        Add relationship triples from a parent direct or indirect container.

        @param cont_rsrc (rdflib.resource.Resouce)  The container resource.
        '''
        cont_p = set(cont_rsrc.metadata.graph.predicates())

        self._logger.info('Checking direct or indirect containment.')
        self._logger.debug('Parent predicates: {}'.format(cont_p))

        add_trp = {(self.urn, nsc['fcrepo'].hasParent, cont_rsrc.urn)}

        if self.MBR_RSRC_URI in cont_p and self.MBR_REL_URI in cont_p:
            s = cont_rsrc.metadata.value(self.MBR_RSRC_URI).identifier
            p = cont_rsrc.metadata.value(self.MBR_REL_URI).identifier

            if cont_rsrc.metadata[RDF.type : nsc['ldp'].DirectContainer]:
                self._logger.info('Parent is a direct container.')

                self._logger.debug('Creating DC triples.')
                o = self.urn

            elif cont_rsrc.metadata[RDF.type : nsc['ldp'].IndirectContainer] \
                   and self.INS_CNT_REL_URI in cont_p:
                self._logger.info('Parent is an indirect container.')
                cont_rel_uri = cont_rsrc.metadata.value(
                        self.INS_CNT_REL_URI).identifier
                o = self.provided_imr.value(cont_rel_uri).identifier
                self._logger.debug('Target URI: {}'.format(o))
                self._logger.debug('Creating IC triples.')

            target_rsrc = LdpFactory.from_stored(g.tbox.uri_to_uuid(s))
            target_rsrc._modify_rsrc(self.RES_UPDATED, add_trp={(s, p, o)})

        self._modify_rsrc(self.RES_UPDATED, add_trp=add_trp)


    # @TODO reenable at request level.
    #def _send_event_msg(self, remove_trp, add_trp, metadata):
    #    '''
    #    Break down delta triples, find subjects and send event message.
    #    '''
    #    remove_grp = groupby(remove_trp, lambda x : x[0])
    #    remove_dict = { k[0] : k[1] for k in remove_grp }

    #    add_grp = groupby(add_trp, lambda x : x[0])
    #    add_dict = { k[0] : k[1] for k in add_grp }

    #    subjects = set(remove_dict.keys()) | set(add_dict.keys())
    #    for rsrc_uri in subjects:
    #        self._logger.info('subject: {}'.format(rsrc_uri))
    #        #current_app.messenger.send
