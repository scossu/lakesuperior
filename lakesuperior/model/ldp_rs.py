from copy import deepcopy

import arrow

from flask import current_app
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF, XSD
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.term import URIRef, Literal, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.model.ldpr import Ldpr, transactional, must_exist
from lakesuperior.exceptions import ResourceNotExistsError, \
        ServerManagedTermError, SingleSubjectError
from lakesuperior.toolbox import Toolbox

class LdpRs(Ldpr):
    '''LDP-RS (LDP RDF source).

    Definition: https://www.w3.org/TR/ldp/#ldprs
    '''

    DEFAULT_USER = Literal('BypassAdmin')

    base_types = {
        nsc['fcrepo'].Resource,
        nsc['ldp'].Resource,
        nsc['ldp'].RDFSource,
    }


    ## LDP METHODS ##

    def get(self, repr_opts):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_GET
        '''
        return Toolbox().globalize_rsrc(self.imr)


    @transactional
    def post(self, data, format='text/turtle', handling=None):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

        Perform a POST action after a valid resource URI has been found.
        '''
        return self._create_or_update_rsrc(data, format, handling,
                create_only=True)


    @transactional
    def put(self, data, format='text/turtle', handling=None):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT
        '''
        return self._create_or_update_rsrc(data, format, handling)


    @transactional
    @must_exist
    def patch(self, update_str):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH

        Update an existing resource by applying a SPARQL-UPDATE query.

        @param update_str (string) SPARQL-Update staements.
        '''
        delta = self._sparql_delta(update_str.replace('<>', self.urn.n3()))

        return self.rdfly.modify_dataset(*delta)


    ## PROTECTED METHODS ##

    def _create_or_update_rsrc(self, data, format, handling,
            create_only=False):
        '''
        Create or update a resource. PUT and POST methods, which are almost
        identical, are wrappers for this method.

        @param data (string) RDF data to parse for insertion.
        @param format(string) MIME type of RDF data.
        @param handling (sting) One of `strict` or `lenient`. This determines
        how to handle provided server-managed triples. If `strict` is selected,
        any server-managed triple  included in the input RDF will trigger an
        exception. If `lenient`, server-managed triples are ignored.
        @param create_only (boolean) Whether the operation is a create-only
        one (i.e. POST) or a create-or-update one (i.e. PUT).
        '''
        g = Graph()
        if data:
            g.parse(data=data, format=format, publicID=self.urn)

        self.provided_imr = Resource(self._check_mgd_terms(g, handling),
                self.urn)

        create = create_only or not self.is_stored
        self._add_srv_mgd_triples(create)
        self._ensure_single_subject_rdf(self.provided_imr.graph)
        ref_int = self.rdfly.conf['referential_integrity']
        if ref_int:
            self._check_ref_int(ref_int)

        if create:
            ev_type = self._create_rsrc()
        else:
            ev_type = self._replace_rsrc()

        self._set_containment_rel()

        return ev_type


    ## PROTECTED METHODS ##


    def _check_mgd_terms(self, g, handling='strict'):
        '''
        Check whether server-managed terms are in a RDF payload.

        @param handling (string) One of `strict` (the default) or `lenient`.
        `strict` raises an error if a server-managed term is in the graph.
        `lenient` removes all sever-managed triples encountered.
        '''
        offending_subjects = set(g.subjects()) & srv_mgd_subjects
        if offending_subjects:
            if handling=='strict':
                raise ServerManagedTermError(offending_subjects, 's')
            else:
                for s in offending_subjects:
                    self._logger.info('Removing offending subj: {}'.format(s))
                    g.remove((s, None, None))

        offending_predicates = set(g.predicates()) & srv_mgd_predicates
        if offending_predicates:
            if handling=='strict':
                raise ServerManagedTermError(offending_predicates, 'p')
            else:
                for p in offending_predicates:
                    self._logger.info('Removing offending pred: {}'.format(p))
                    g.remove((None, p, None))

        offending_types = set(g.objects(predicate=RDF.type)) & srv_mgd_types
        if offending_types:
            if handling=='strict':
                raise ServerManagedTermError(offending_types, 't')
            else:
                for t in offending_types:
                    self._logger.info('Removing offending type: {}'.format(t))
                    g.remove((None, RDF.type, t))

        self._logger.debug('Sanitized graph: {}'.format(g.serialize(
            format='turtle').decode('utf-8')))
        return g


    def _add_srv_mgd_triples(self, create=False):
        '''
        Add server-managed triples to a provided IMR.

        @param create (boolean) Whether the resource is being created.
        '''
        # Message digest.
        cksum = Toolbox().rdf_cksum(self.provided_imr.graph)
        self.provided_imr.set(nsc['premis'].hasMessageDigest,
                URIRef('urn:sha1:{}'.format(cksum)))

        # Create and modify timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)
        if create:
            self.provided_imr.set(nsc['fcrepo'].created, ts)
            self.provided_imr.set(nsc['fcrepo'].createdBy, self.DEFAULT_USER)

        self.provided_imr.set(nsc['fcrepo'].lastModified, ts)
        self.provided_imr.set(nsc['fcrepo'].lastModifiedBy, self.DEFAULT_USER)

        # Base LDP types.
        for t in self.base_types:
            self.provided_imr.add(RDF.type, t)


    def _sparql_delta(self, q, handling=None):
        '''
        Calculate the delta obtained by a SPARQL Update operation.

        This is a critical component of the SPARQL query prcess and does a
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

        @return tuple Remove and add triples. These can be used with
        `BaseStoreLayout.update_resource` and/or recorded as separate events in
        a provenance tracking system.
        '''

        pre_g = self.imr.graph

        post_g = deepcopy(pre_g)
        post_g.update(q)

        remove = pre_g - post_g
        add = post_g - pre_g

        self._logger.info('Removing: {}'.format(
            remove.serialize(format='turtle').decode('utf8')))
        self._logger.info('Adding: {}'.format(
            add.serialize(format='turtle').decode('utf8')))

        remove = self._check_mgd_terms(remove, handling)
        add = self._check_mgd_terms(add, handling)

        return remove, add


    def _ensure_single_subject_rdf(self, g):
        '''
        Ensure that a RDF payload for a POST or PUT has a single resource.
        '''
        for s in set(g.subjects()):
            if not s == self.urn:
                raise SingleSubjectError(s, self.uuid)


    def _check_ref_int(self, config):
        g = self.provided_imr.graph

        for o in g.objects():
            if isinstance(o, URIRef) and str(o).startswith(Toolbox().base_url)\
                    and not self.rdfly.ask_rsrc_exists(o):
                if config == 'strict':
                    raise RefIntViolationError(o)
                else:
                    self._logger.info(
                            'Removing link to non-existent repo resource: {}'
                            .format(o))
                    g.remove((None, None, o))


class Ldpc(LdpRs):
    '''LDPC (LDP Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].Container,
        })




class LdpBc(Ldpc):
    '''LDP-BC (LDP Basic Container).'''
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].BasicContainer,
        })



class LdpDc(Ldpc):
    '''LDP-DC (LDP Direct Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].DirectContainer,
        })



class LdpIc(Ldpc):
    '''LDP-IC (LDP Indirect Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].IndirectContainer,
        })





