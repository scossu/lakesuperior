from copy import deepcopy

import arrow

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
from lakesuperior.util.digest import Digest
from lakesuperior.util.translator import Translator

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
        return Translator.globalize_rsrc(self.imr)


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
        delta = self._sparql_delta(update_str)

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
        g = Graph().parse(data=data, format=format, publicID=self.urn)

        imr = Resource(self._check_mgd_terms(g, handling), self.urn)
        imr = self._add_srv_mgd_triples(imr, create=True)
        self._ensure_single_subject_rdf(imr.graph)

        if create_only:
            res = self.rdfly.create_rsrc(imr)
        else:
            res = self.rdfly.create_or_replace_rsrc(imr)

        self._set_containment_rel()

        return res


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
                    g.remove((s, Variable('p'), Variable('o')))

        offending_predicates = set(g.predicates()) & srv_mgd_predicates
        if offending_predicates:
            if handling=='strict':
                raise ServerManagedTermError(offending_predicates, 'p')
            else:
                for p in offending_predicates:
                    g.remove((Variable('s'), p, Variable('o')))

        offending_types = set(g.objects(predicate=RDF.type)) & srv_mgd_types
        if offending_types:
            if handling=='strict':
                raise ServerManagedTermError(offending_types, 't')
            else:
                for t in offending_types:
                    g.remove((Variable('s'), RDF.type, t))

        return g


    def _add_srv_mgd_triples(self, rsrc, create=False):
        '''
        Add server-managed triples to a resource.

        @param create (boolean) Whether the resource is being created.
        '''
        # Message digest.
        cksum = Digest.rdf_cksum(rsrc.graph)
        rsrc.set(nsc['premis'].hasMessageDigest,
                URIRef('urn:sha1:{}'.format(cksum)))

        # Create and modify timestamp.
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)
        if create:
            rsrc.set(nsc['fcrepo'].created, ts)
            rsrc.set(nsc['fcrepo'].createdBy, self.DEFAULT_USER)

        rsrc.set(nsc['fcrepo'].lastModified, ts)
        rsrc.set(nsc['fcrepo'].lastModifiedBy, self.DEFAULT_USER)

        # Base LDP types.
        for t in self.base_types:
            rsrc.add(RDF.type, t)

        return rsrc


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
            if not s == self.uri:
                return SingleSubjectError(s, self.uri)


class Ldpc(LdpRs):
    '''LDPC (LDP Container).'''

    def __init__(self, uuid):
        super().__init__(uuid)
        self.base_types.update({
            nsc['ldp'].Container,
        })




class LdpBc(Ldpc):
    '''LDP-BC (LDP Basic Container).'''
    def __init__(self, uuid):
        super().__init__(uuid)
        self.base_types.update({
            nsc['ldp'].BasicContainer,
        })



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





