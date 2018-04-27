import logging
import re

from abc import ABCMeta
from collections import defaultdict
from urllib.parse import urldefrag
from uuid import uuid4

import arrow

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF

from lakesuperior import env, thread_env
from lakesuperior.globals import (
    RES_CREATED, RES_DELETED, RES_UPDATED, ROOT_UID)
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.srv_mgd_terms import (
    srv_mgd_subjects, srv_mgd_predicates, srv_mgd_types)
from lakesuperior.exceptions import (
    InvalidResourceError, RefIntViolationError, ResourceNotExistsError,
    ServerManagedTermError, TombstoneError)
from lakesuperior.store.ldp_rs.rsrc_centric_layout import VERS_CONT_LABEL
from lakesuperior.toolbox import Toolbox


rdfly = env.app_globals.rdfly
logger = logging.getLogger(__name__)


class Ldpr(metaclass=ABCMeta):
    """
    LDPR (LDP Resource).

    This class and related subclasses contain the implementation pieces of
    the `LDP Resource <https://www.w3.org/TR/ldp/#ldpr-resource>`__
    specifications, according to their `inheritance graph
    <https://www.w3.org/TR/ldp/#fig-ldpc-types>`__.

    **Note**: Even though LdpNr (which is a subclass of Ldpr) handles binary
    files, it still has an RDF representation in the triplestore. Hence, some
    of the RDF-related methods are defined in this class rather than in
    :class:`~lakesuperior.model.ldp_rs.LdpRs`.

    **Note:** Only internal facing (``info:fcres``-prefixed) URIs are handled
    in this class. Public-facing URI conversion is handled in the
    :mod:`~lakesuperior.endpoints.ldp` module.
    """

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

    DEFAULT_USER = Literal('BypassAdmin')
    """
    Default user to be used for the `createdBy` and `lastUpdatedBy` if a user
    is not provided.
    """

    base_types = {
        nsc['fcrepo'].Resource,
        nsc['ldp'].Resource,
        nsc['ldp'].RDFSource,
    }
    """RDF Types that populate a new resource."""

    protected_pred = (
        nsc['fcrepo'].created,
        nsc['fcrepo'].createdBy,
        nsc['ldp'].contains,
    )
    """Predicates that do not get removed when a resource is replaced."""

    smt_allow_on_create = {
        nsc['ldp'].DirectContainer,
        nsc['ldp'].IndirectContainer,
    }
    """
    Server-managed RDF types ignored in the RDF payload if the resource is
    being created. N.B. These still raise an error if the resource exists.
    """

    delete_preds_on_replace = {
        nsc['ebucore'].hasMimeType,
        nsc['fcrepo'].lastModified,
        nsc['fcrepo'].lastModifiedBy,
        nsc['premis'].hasSize,
        nsc['premis'].hasMessageDigest,
    }
    """Predicates to remove when a resource is replaced."""

    _ignore_version_preds = {
        nsc['fcrepo'].hasParent,
        nsc['fcrepo'].hasVersions,
        nsc['fcrepo'].hasVersion,
        nsc['premis'].hasMessageDigest,
        nsc['ldp'].contains,
    }
    """Predicates that don't get versioned."""

    _ignore_version_types = {
        nsc['fcrepo'].Binary,
        nsc['fcrepo'].Container,
        nsc['fcrepo'].Pairtree,
        nsc['fcrepo'].Resource,
        nsc['fcrepo'].Version,
        nsc['ldp'].BasicContainer,
        nsc['ldp'].Container,
        nsc['ldp'].DirectContainer,
        nsc['ldp'].Resource,
        nsc['ldp'].RDFSource,
        nsc['ldp'].NonRDFSource,
    }
    """RDF types that don't get versioned."""


    ## MAGIC METHODS ##

    def __init__(self, uid, repr_opts={}, provided_imr=None, **kwargs):
        """
        Instantiate an in-memory LDP resource.

        :param str uid: uid of the resource. If None (must be explicitly
        set) it refers to the root node. It can also be the full URI or URN,
        in which case it will be converted.
        :param dict repr_opts: Options used to retrieve the IMR. See
        `parse_rfc7240` for format details.
        :param str provd_rdf: RDF data provided by the client in
        operations such as `PUT` or `POST`, serialized as a string. This sets
        the `provided_imr` property.
        """
        self.uid = (
            rdfly.uri_to_uid(uid) if isinstance(uid, URIRef) else uid)
        self.uri = nsc['fcres'][uid]
        # @FIXME Not ideal, should separate app-context dependent functions in
        # a different toolbox.
        self.tbox = Toolbox()

        self.provided_imr = provided_imr

        # Disable all internal checks e.g. for raw I/O.


    @property
    def rsrc(self):
        """
        The RDFLib resource representing this LDPR. This is a live
        representation of the stored data if present.

        :rtype: rdflib.Resource
        """
        if not hasattr(self, '_rsrc'):
            self._rsrc = rdfly.ds.resource(self.uri)

        return self._rsrc


    @property
    def imr(self):
        """
        In-Memory Resource.

        This is a copy of the resource extracted from the graph store. It is a
        graph resource whose identifier is the URI of the resource.

        >>> rsrc = rsrc_api.get('/')
        >>> rsrc.imr.identifier
        rdflib.term.URIRef('info:fcres/')
        >>> rsrc.imr.value(rsrc.imr.identifier, nsc['fcrepo'].lastModified)
        rdflib.term.Literal(
            '2018-04-03T05:20:33.774746+00:00',
            datatype=rdflib.term.URIRef(
                'http://www.w3.org/2001/XMLSchema#dateTime'))

        The IMR can be read and manipulated, as well as used to
        update the stored resource.

        :rtype: rdflib.Graph
        :raise lakesuperior.exceptions.ResourceNotExistsError: If the resource
            is not stored (yet).
        """
        if not hasattr(self, '_imr'):
            if hasattr(self, '_imr_options'):
                logger.debug(
                    'Getting RDF representation for resource {}'
                    .format(self.uid))
                #logger.debug('IMR options:{}'.format(self._imr_options))
                imr_options = self._imr_options
            else:
                imr_options = {}
            options = dict(imr_options, strict=True)
            self._imr = rdfly.get_imr(self.uid, **options)

        return self._imr


    @imr.setter
    def imr(self, v):
        """
        Replace in-memory buffered resource.

        :param v: New set of triples to populate the IMR with.
        :type v: set or rdflib.Graph
        """
        self._imr = Graph(identifier=self.uri)
        self._imr += v


    @imr.deleter
    def imr(self):
        """
        Delete in-memory buffered resource.
        """
        delattr(self, '_imr')


    @property
    def metadata(self):
        """
        Get resource metadata.
        """
        if not hasattr(self, '_metadata'):
            if hasattr(self, '_imr'):
                logger.info('Metadata is IMR.')
                self._metadata = self._imr
            else:
                logger.info(
                    'Getting metadata for resource {}'.format(self.uid))
                self._metadata = rdfly.get_metadata(self.uid)

        return self._metadata


    @metadata.setter
    def metadata(self, rsrc):
        """
        Set resource metadata.
        """
        if not isinstance(rsrc, Resource):
            raise TypeError('Provided metadata is not a Resource object.')
        self._metadata = rsrc


    @property
    def out_graph(self):
        """
        Retun a graph of the resource's IMR formatted for output.
        """
        out_gr = Graph(identifier=self.uri)

        for t in self.imr:
            if (
                # Exclude digest hash and version information.
                t[1] not in {
                    #nsc['premis'].hasMessageDigest,
                    nsc['fcrepo'].hasVersion,
                }
            ) and (
                # Only include server managed triples if requested.
                self._imr_options.get('incl_srv_mgd', True) or
                not self._is_trp_managed(t)
            ):
                out_gr.add(t)

        return out_gr


    @property
    def version_info(self):
        """
        Return version metadata (`fcr:versions`).
        """
        if not hasattr(self, '_version_info'):
            try:
                self._version_info = rdfly.get_version_info(self.uid)
            except ResourceNotExistsError as e:
                self._version_info = Graph(identifier=self.uri)

        return self._version_info


    @property
    def version_uids(self):
        """
        Return a generator of version UIDs (relative to their parent resource).
        """
        gen = self.version_info[
            self.uri:
            nsc['fcrepo'].hasVersion / nsc['fcrepo'].hasVersionLabel:]

        return {str(uid) for uid in gen}


    @property
    def is_stored(self):
        if not hasattr(self, '_is_stored'):
            if hasattr(self, '_imr'):
                self._is_stored = len(self.imr) > 0
            else:
                self._is_stored = rdfly.ask_rsrc_exists(self.uid)

        return self._is_stored


    @property
    def types(self):
        """All RDF types.

        :rtype: set(rdflib.term.URIRef)
        """
        if not hasattr(self, '_types'):
            if len(self.metadata):
                metadata = self.metadata
            elif getattr(self, 'provided_imr', None) and \
                    len(self.provided_imr):
                metadata = self.provided_imr
            else:
                return set()

            self._types = set(metadata[self.uri: RDF.type])

        return self._types


    @property
    def ldp_types(self):
        """The LDP types.

        :rtype: set(rdflib.term.URIRef)
        """
        if not hasattr(self, '_ldp_types'):
            self._ldp_types = {t for t in self.types if nsc['ldp'] in t}

        return self._ldp_types


    ## LDP METHODS ##

    def head(self):
        """
        Return values for the headers.
        """
        out_headers = defaultdict(list)

        digest = self.metadata.value(self.uri, nsc['premis'].hasMessageDigest)
        if digest:
            etag = digest.identifier.split(':')[-1]
            out_headers['ETag'] = 'W/"{}"'.format(etag),

        last_updated_term = self.metadata.value(
            self.uri, nsc['fcrepo'].lastModified)
        if last_updated_term:
            out_headers['Last-Modified'] = arrow.get(last_updated_term)\
                .format('ddd, D MMM YYYY HH:mm:ss Z')

        for t in self.ldp_types:
            out_headers['Link'].append(
                '{};rel="type"'.format(t.n3()))

        return out_headers


    def get_version(self, ver_uid, **kwargs):
        """
        Get a version by label.
        """
        return rdfly.get_imr(self.uid, ver_uid, **kwargs)


    def create_or_replace(self, create_only=False):
        """
        Create or update a resource. PUT and POST methods, which are almost
        identical, are wrappers for this method.

        :param boolean create_only: Whether this is a create-only operation.
        """
        create = create_only or not self.is_stored

        ev_type = RES_CREATED if create else RES_UPDATED
        self._add_srv_mgd_triples(create)
        ref_int = rdfly.config['referential_integrity']
        if ref_int:
            self._check_ref_int(ref_int)

        # Delete existing triples if replacing.
        if not create:
            rdfly.truncate_rsrc(self.uid)

        remove_trp = {
            (self.uri, pred, None) for pred in self.delete_preds_on_replace}
        add_trp = (
            set(self.provided_imr) |
            self._containment_rel(create))

        self.modify(ev_type, remove_trp, add_trp)
        new_gr = Graph(identifier=self.uri)
        for trp in add_trp:
            new_gr.add(trp)

        self.imr = new_gr

        return ev_type


    def bury(self, inbound, tstone_pointer=None):
        """
        Delete a single resource and create a tombstone.

        :param bool inbound: Whether inbound relationships are
            removed. If ``False``, resources will keep referring
            to the deleted resource; their link will point to a tombstone
            (which will raise a ``TombstoneError`` in the Python API or a
            ``410 Gone`` in the LDP API).
        :param rdflib.URIRef tstone_pointer: If set to a URI, this creates a
            pointer to the tombstone of the resource that used to contain the
            deleted resource. Otherwise the deleted resource becomes a
            tombstone.
        """
        logger.info('Burying resource {}'.format(self.uid))
        # ldp:Resource is also used in rdfly.ask_rsrc_exists.
        remove_trp = {
            (nsc['fcrepo'].uid, nsc['rdf'].type, nsc['ldp'].Resource)
        }

        if tstone_pointer:
            add_trp = {
                (self.uri, nsc['fcsystem'].tombstone, tstone_pointer)}
        else:
            add_trp = {
                (self.uri, RDF.type, nsc['fcsystem'].Tombstone),
                (self.uri, nsc['fcsystem'].buried, thread_env.timestamp_term),
            }

        # Bury descendants.
        from lakesuperior.model.ldp_factory import LdpFactory
        for desc_uri in rdfly.get_descendants(self.uid):
            try:
                desc_rsrc = LdpFactory.from_stored(
                    env.app_globals.rdfly.uri_to_uid(desc_uri),
                    repr_opts={'incl_children' : False})
            except (TombstoneError, ResourceNotExistsError):
                continue
            desc_rsrc.bury(inbound, tstone_pointer=self.uri)

        # Cut inbound relationships
        if inbound:
            for ib_rsrc_uri in self.imr.subjects(None, self.uri):
                remove_trp = {(ib_rsrc_uri, None, self.uri)}
                ib_rsrc = Ldpr(ib_rsrc_uri)
                # To preserve inbound links in history, create a snapshot
                ib_rsrc.create_version()
                ib_rsrc.modify(RES_UPDATED, remove_trp)

        self.modify(RES_DELETED, remove_trp, add_trp)

        return RES_DELETED


    def forget(self, inbound=True):
        """
        Remove all traces of a resource and versions.
        """
        logger.info('Purging resource {}'.format(self.uid))
        refint = rdfly.config['referential_integrity']
        inbound = True if refint else inbound

        for desc_uri in rdfly.get_descendants(self.uid):
            rdfly.forget_rsrc(rdfly.uri_to_uid(desc_uri), inbound)

        rdfly.forget_rsrc(self.uid, inbound)

        return RES_DELETED


    def resurrect(self):
        """
        Resurrect a resource from a tombstone.
        """
        remove_trp = {
            (self.uri, nsc['rdf'].type, nsc['fcsystem'].Tombstone),
            (self.uri, nsc['fcsystem'].tombstone, None),
            (self.uri, nsc['fcsystem'].buried, None),
        }
        add_trp = {
            (self.uri, nsc['rdf'].type, nsc['ldp'].Resource),
        }

        self.modify(RES_CREATED, remove_trp, add_trp)

        # Resurrect descendants.
        from lakesuperior.model.ldp_factory import LdpFactory
        descendants = env.app_globals.rdfly.get_descendants(self.uid)
        for desc_uri in descendants:
            LdpFactory.from_stored(
                    rdfly.uri_to_uid(desc_uri), strict=False).resurrect()

        return self.uri


    def create_version(self, ver_uid=None):
        """
        Create a new version of the resource.

        **Note:** This creates an event only for the resource being updated
        (due to the added `hasVersion` triple and possibly to the
        ``hasVersions`` one) but not for the version being created.

        :param str ver_uid: Version UID. If already existing, a new version UID
            is minted.
        """
        if not ver_uid or ver_uid in self.version_uids:
            ver_uid = str(uuid4())

        # Create version resource from copying the current state.
        logger.info(
            'Creating version snapshot {} for resource {}.'.format(
                ver_uid, self.uid))
        ver_add_gr = set()
        vers_uid = '{}/{}'.format(self.uid, VERS_CONT_LABEL)
        ver_uid = '{}/{}'.format(vers_uid, ver_uid)
        ver_uri = nsc['fcres'][ver_uid]
        ver_add_gr.add((ver_uri, RDF.type, nsc['fcrepo'].Version))
        for t in self.imr:
            if (
                t[1] == RDF.type and t[2] in self._ignore_version_types
            ) or t[1] in self._ignore_version_preds:
                pass
            else:
                ver_add_gr.add((
                    self.tbox.replace_term_domain(t[0], self.uri, ver_uri),
                    t[1], t[2]))

        rdfly.modify_rsrc(ver_uid, add_trp=ver_add_gr)

        # Update resource admin data.
        rsrc_add_gr = {
            (self.uri, nsc['fcrepo'].hasVersion, ver_uri),
            (self.uri, nsc['fcrepo'].hasVersions, nsc['fcres'][vers_uid]),
        }
        self.modify(RES_UPDATED, add_trp=rsrc_add_gr)

        return ver_uid




    def revert_to_version(self, ver_uid, backup=True):
        """
        Revert to a previous version.

        :param str ver_uid: Version UID.
        :param boolean backup: Whether to create a backup snapshot. Default is
            True.
        """
        # Create a backup snapshot.
        if backup:
            self.create_version()

        ver_gr = rdfly.get_imr(
            self.uid, ver_uid=ver_uid, incl_children=False)
        self.provided_imr = Graph(identifier=self.uri)

        for t in ver_gr:
            if not self._is_trp_managed(t):
                self.provided_imr.add((self.uri, t[1], t[2]))
            # @TODO Check individual objects: if they are repo-managed URIs
            # and not existing or tombstones, they are not added.

        return self.create_or_replace(create_only=False)


    def check_mgd_terms(self, trp):
        """
        Check whether server-managed terms are in a RDF payload.

        :param rdflib.Graph trp: The graph to validate.
        """
        subjects = {t[0] for t in trp}
        offending_subjects = subjects & srv_mgd_subjects
        if offending_subjects:
            if self.handling == 'strict':
                raise ServerManagedTermError(offending_subjects, 's')
            else:
                for s in offending_subjects:
                    logger.info('Removing offending subj: {}'.format(s))
                    for t in trp:
                        if t[0] == s:
                            trp.remove(t)

        predicates = {t[1] for t in trp}
        offending_predicates = predicates & srv_mgd_predicates
        # Allow some predicates if the resource is being created.
        if offending_predicates:
            if self.handling == 'strict':
                raise ServerManagedTermError(offending_predicates, 'p')
            else:
                for p in offending_predicates:
                    logger.info('Removing offending pred: {}'.format(p))
                    for t in trp:
                        if t[1] == p:
                            trp.remove(t)

        types = {t[2] for t in trp if t[1] == RDF.type}
        offending_types = types & srv_mgd_types
        if not self.is_stored:
            offending_types -= self.smt_allow_on_create
        if offending_types:
            if self.handling == 'strict':
                raise ServerManagedTermError(offending_types, 't')
            else:
                for to in offending_types:
                    logger.info('Removing offending type: {}'.format(to))
                    for t in trp:
                        if t[1] == RDF.type and t[2] == to:
                            trp.remove(t)

        #logger.debug('Sanitized graph: {}'.format(trp.serialize(
        #    format='turtle').decode('utf-8')))
        return trp


    def sparql_delta(self, qry_str):
        """
        Calculate the delta obtained by a SPARQL Update operation.

        This is a critical component of the SPARQL update prcess and does a
        couple of things:

        1. It ensures that no resources outside of the subject of the request
        are modified (e.g. by variable subjects)
        2. It verifies that none of the terms being modified is server managed.

        This method extracts an in-memory copy of the resource and performs the
        query on that once it has checked if any of the server managed terms is
        in the delta. If it is, it raises an exception.

        NOTE: This only checks if a server-managed term is effectively being
        modified. If a server-managed term is present in the query but does not
        cause any change in the updated resource, no error is raised.

        :rtype: tuple(rdflib.Graph)
        :return: Remove and add graphs. These can be used
            with ``BaseStoreLayout.update_resource`` and/or recorded as separate
            events in a provenance tracking system.
        """
        logger.debug('Provided SPARQL query: {}'.format(qry_str))
        # Workaround for RDFLib bug. See
        # https://github.com/RDFLib/rdflib/issues/824
        qry_str = (
                re.sub('<#([^>]+)>', '<{}#\\1>'.format(self.uri), qry_str)
                .replace('<>', '<{}>'.format(self.uri)))
        pre_gr = Graph(identifier=self.uri)
        pre_gr += self.imr
        post_gr = Graph(identifier=self.uri)
        post_gr += self.imr

        post_gr.update(qry_str)

        remove_gr, add_gr = self._dedup_deltas(pre_gr, post_gr)

        #logger.debug('Removing: {}'.format(
        #    remove_gr.serialize(format='turtle').decode('utf8')))
        #logger.debug('Adding: {}'.format(
        #    add_gr.serialize(format='turtle').decode('utf8')))

        remove_trp = self.check_mgd_terms(set(remove_gr))
        add_trp = self.check_mgd_terms(set(add_gr))

        return remove_trp, add_trp


    ## PROTECTED METHODS ##

    def _is_trp_managed(self, t):
        """
        Whether a triple is server-managed.

        :param tuple t: Triple as a 3-tuple of terms.

        :rtype: boolean
        """
        return t[1] in srv_mgd_predicates or (
            t[1] == RDF.type and t[2] in srv_mgd_types)


    def modify(
            self, ev_type, remove_trp=set(), add_trp=set()):
        """
        Low-level method to modify a graph for a single resource.

        This is a crucial point for messaging. Any write operation on the RDF
        store that needs to be notified should be performed by invoking this
        method.

        :param ev_type: The type of event (create, update,
            delete) or None. In the latter case, no notification is sent.
        :type ev_type: str or None
        :param set remove_trp: Triples to be removed.
        :param set add_trp: Triples to be added.
        """
        rdfly.modify_rsrc(self.uid, remove_trp, add_trp)
        # Clear IMR buffer.
        if hasattr(self, '_imr'):
            delattr(self, '_imr')
            try:
                self.imr
            except (ResourceNotExistsError, TombstoneError):
                pass

        if (
                ev_type is not None and
                env.app_globals.config['application'].get('messaging')):
            logger.debug('Enqueuing message for {}'.format(self.uid))
            self._enqueue_msg(ev_type, remove_trp, add_trp)


    def _enqueue_msg(self, ev_type, remove_trp=None, add_trp=None):
        """
        Compose a message about a resource change.

        The message is enqueued for asynchronous processing.

        :param str ev_type: The event type. See global constants.
        :param set remove_trp: Triples removed. Only used if the
        """
        try:
            rsrc_type = tuple(str(t) for t in self.types)
            actor = self.metadata.value(self.uri, nsc['fcrepo'].createdBy)
        except (ResourceNotExistsError, TombstoneError):
            rsrc_type = ()
            actor = None
            for t in add_trp:
                if t[1] == RDF.type:
                    rsrc_type.add(t[2])
                elif actor is None and t[1] == nsc['fcrepo'].createdBy:
                    actor = t[2]

        env.app_globals.changelog.append((set(remove_trp), set(add_trp), {
            'ev_type': ev_type,
            'timestamp': thread_env.timestamp.format(),
            'rsrc_type': rsrc_type,
            'actor': actor,
        }))


    def _check_ref_int(self, config):
        """
        Check referential integrity of a resource.

        :param str config: If set to ``strict``, a
           :class:`lakesuperior.exceptions.RefIntViolationError` is raised.
           Otherwise, the violation is simply logged.
        """
        for o in self.provided_imr.objects():
            if(
                    isinstance(o, URIRef) and
                    str(o).startswith(nsc['fcres']) and
                    urldefrag(o).url.rstrip('/') != str(self.uri)):
                obj_uid = rdfly.uri_to_uid(o)
                if not rdfly.ask_rsrc_exists(obj_uid):
                    if config == 'strict':
                        raise RefIntViolationError(obj_uid)
                    else:
                        logger.info(
                            'Removing link to non-existent repo resource: {}'
                            .format(obj_uid))
                        self.provided_imr.remove((None, None, o))


    def _add_srv_mgd_triples(self, create=False):
        """
        Add server-managed triples to a provided IMR.

        :param create: Whether the resource is being created.
        """
        # Base LDP types.
        for t in self.base_types:
            self.provided_imr.add((self.uri, RDF.type, t))

        # Message digest.
        cksum = self.tbox.rdf_cksum(self.provided_imr)
        self.provided_imr.set((
            self.uri, nsc['premis'].hasMessageDigest,
            URIRef('urn:sha1:{}'.format(cksum))))

        # Create and modify timestamp.
        if create:
            self.provided_imr.set((
                self.uri, nsc['fcrepo'].created, thread_env.timestamp_term))
            self.provided_imr.set((
                self.uri, nsc['fcrepo'].createdBy, self.DEFAULT_USER))
        else:
            self.provided_imr.set((
                self.uri, nsc['fcrepo'].created, self.metadata.value(
                    self.uri, nsc['fcrepo'].created)))
            self.provided_imr.set((
                self.uri, nsc['fcrepo'].createdBy, self.metadata.value(
                    self.uri, nsc['fcrepo'].createdBy)))

        self.provided_imr.set((
            self.uri, nsc['fcrepo'].lastModified, thread_env.timestamp_term))
        self.provided_imr.set((
            self.uri, nsc['fcrepo'].lastModifiedBy, self.DEFAULT_USER))


    def _containment_rel(self, create, ignore_type=True):
        """Find the closest parent in the path indicated by the uid and
        establish a containment triple.

        Check the path-wise parent of the new resource. If it exists, add the
        containment relationship with this UID. Otherwise, create a container
        resource as the parent.
        This function may recurse up the path tree until an existing container
        is found.

        E.g. if only fcres:/a exists:
        - If ``fcres:/a/b/c/d`` is being created, a becomes container of
          ``fcres:/a/b/c/d``. Also, containers are created for fcres:a/b and
          ``fcres:/a/b/c``.
        - If ``fcres:/e`` is being created, the root node becomes container of
          ``fcres:/e``.

        :param bool create: Whether the resource is being created. If false,
        the parent container is not updated.
        "param bool ignore_type: If False (the default), an exception is raised
        if trying to create a resource under a non-container. This can be
        overridden in special cases (e.g. when migrating a repository in which
        a LDP-NR has "children" under ``fcr:versions``) by setting this to
        True.
        """
        from lakesuperior.model.ldp_factory import LdpFactory

        if '/' in self.uid.lstrip('/'):
            # Traverse up the hierarchy to find the parent.
            path_components = self.uid.lstrip('/').split('/')
            cnd_parent_uid = '/' + '/'.join(path_components[:-1])
            if rdfly.ask_rsrc_exists(cnd_parent_uid):
                parent_rsrc = LdpFactory.from_stored(cnd_parent_uid)
                if (
                        not ignore_type
                        and nsc['ldp'].Container not in parent_rsrc.types):
                    raise InvalidResourceError(
                        cnd_parent_uid, 'Parent {} is not a container.')

                parent_uid = cnd_parent_uid
            else:
                parent_rsrc = LdpFactory.new_container(cnd_parent_uid)
                # This will trigger this method again and recurse until an
                # existing container or the root node is reached.
                parent_rsrc.create_or_replace()
                parent_uid = parent_rsrc.uid
        else:
            parent_uid = ROOT_UID

        parent_rsrc = LdpFactory.from_stored(
            parent_uid, repr_opts={'incl_children': False}, handling='none')

        # Only update parent if the resource is new.
        if create:
            add_gr = Graph()
            add_gr.add(
                (nsc['fcres'][parent_uid], nsc['ldp'].contains, self.uri))
            parent_rsrc.modify(RES_UPDATED, add_trp=add_gr)

        # Direct or indirect container relationship.
        return self._add_ldp_dc_ic_rel(parent_rsrc)


    def _dedup_deltas(self, remove_gr, add_gr):
        """
        Remove duplicate triples from add and remove delta graphs, which would
        otherwise contain unnecessary statements that annul each other.

        :rtype: tuple
        :return: 2 "clean" sets of respectively remove statements and
        add statements.
        """
        return (
            remove_gr - add_gr,
            add_gr - remove_gr
        )


    def _add_ldp_dc_ic_rel(self, cont_rsrc):
        """
        Add relationship triples from a parent direct or indirect container.

        :param rdflib.resource.Resouce cont_rsrc:  The container resource.
        """
        cont_p = set(cont_rsrc.metadata.predicates())

        logger.info('Checking direct or indirect containment.')
        logger.debug('Parent predicates: {}'.format(cont_p))

        add_trp = {(self.uri, nsc['fcrepo'].hasParent, cont_rsrc.uri)}

        if self.MBR_RSRC_URI in cont_p and self.MBR_REL_URI in cont_p:
            from lakesuperior.model.ldp_factory import LdpFactory

            s = cont_rsrc.metadata.value(cont_rsrc.uri, self.MBR_RSRC_URI)
            p = cont_rsrc.metadata.value(cont_rsrc.uri, self.MBR_REL_URI)

            if nsc['ldp'].DirectContainer in cont_rsrc.ldp_types:
                logger.info('Parent is a direct container.')
                logger.debug('Creating DC triples.')
                o = self.uri

            elif nsc['ldp'].IndirectContainer in cont_rsrc.ldp_types:
                logger.info('Parent is an indirect container.')
                cont_rel_uri = cont_rsrc.metadata.value(
                    cont_rsrc.uri, self.INS_CNT_REL_URI)
                o = self.provided_imr.value(self.uri, cont_rel_uri)
                logger.debug('Target URI: {}'.format(o))
                logger.debug('Creating IC triples.')

            target_rsrc = LdpFactory.from_stored(rdfly.uri_to_uid(s))
            target_rsrc.modify(RES_UPDATED, add_trp={(s, p, o)})

        return add_trp
