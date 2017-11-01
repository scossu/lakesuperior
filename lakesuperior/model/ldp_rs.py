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
    RETURN_CHILD_RES_URI = nsc['fcrepo'].EmbedResources
    RETURN_INBOUND_REF_URI = nsc['fcrepo'].InboundReferences
    RETURN_SRV_MGD_RES_URI = nsc['fcrepo'].ServerManaged

    base_types = {
        nsc['ldp'].RDFSource
    }

    std_headers = {
        'Accept-Post' : {
            'text/turtle',
            'text/rdf+n3',
            'text/n3',
            'application/rdf+xml',
            'application/n-triples',
            'application/ld+json',
            'multipart/form-data',
            'application/sparql-update',
        },
        'Accept-Patch' : {
            'application/sparql-update',
        },
    }


    def get(self, pref_return):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_GET
        '''
        kwargs = {}

        minimal = embed_children = incl_inbound = False
        kwargs['incl_srv_mgd'] = True

        if 'value' in pref_return and pref_return['value'] == 'minimal':
            kwargs['minimal'] = True
        else:
            include = pref_return['parameters']['include'].split(' ') \
                    if 'include' in pref_return['parameters'] else []
            omit = pref_return['parameters']['omit'].split(' ') \
                    if 'omit' in pref_return['parameters'] else []

            self._logger.debug('Include: {}'.format(include))
            self._logger.debug('Omit: {}'.format(omit))

            if str(self.RETURN_INBOUND_REF_URI) in include:
                    kwargs['incl_inbound'] = True
            if str(self.RETURN_CHILD_RES_URI) in omit:
                    kwargs['embed_chldren'] = False
            if str(self.RETURN_SRV_MGD_RES_URI) in omit:
                    kwargs['incl_srv_mgd'] = False

        imr = self.rdfly.out_rsrc

        if not imr or not len(imr.graph):
            raise ResourceNotExistsError(self.uri)

        return Translator.globalize_rsrc(imr)


    @transactional
    def post(self, data, format='text/turtle', handling=None):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

        Perform a POST action after a valid resource URI has been found.
        '''
        g = Graph().parse(data=data, format=format, publicID=self.urn)

        imr = Resource(self._check_mgd_terms(g, handling), self.urn)
        imr = self._add_srv_mgd_triples(imr, create=True)
        self._ensure_single_subject_rdf(imr.graph)

        self.rdfly.create_rsrc(imr)

        self._set_containment_rel()


    @transactional
    def put(self, data, format='text/turtle', handling=None):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT
        '''
        g = Graph().parse(data=data, format=format, publicID=self.urn)

        imr = Resource(self._check_mgd_terms(g, handling), self.urn)
        imr = self._add_srv_mgd_triples(imr, create=True)
        self._ensure_single_subject_rdf(imr.graph)

        res = self.rdfly.create_or_replace_rsrc(imr)

        self._set_containment_rel()

        return res


    @transactional
    @must_exist
    def patch(self, data):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH
        '''
        trp_remove, trp_add = self._sparql_delta(data)

        return self.rdfly.modify_rsrc(trp_remove, trp_add)


    ## PROTECTED METHODS ##

    def _check_mgd_terms(self, g, handling='strict'):
        '''
        Check whether server-managed terms are in a RDF payload.
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


    def _add_srv_mgd_triples(self, imr, create=False):
        '''
        Add server-managed triples to a graph.

        @param create (boolean) Whether the resource is being created.
        '''
        # Message digest.
        cksum = Digest.rdf_cksum(imr.graph)
        imr.set(nsc['premis'].hasMessageDigest,
                URIRef('urn:sha1:{}'.format(cksum)))

        # Create and modify timestamp.
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)
        if create:
            imr.set(nsc['fcrepo'].created, ts)
            imr.set(nsc['fcrepo'].createdBy, self.DEFAULT_USER)

        imr.set(nsc['fcrepo'].lastModified, ts)
        imr.set(nsc['fcrepo'].lastModifiedBy, self.DEFAULT_USER)

        # Base LDP types.
        for t in self.base_types:
            imr.add(RDF.type, t)

        return imr


    def _sparql_delta(self, q, handling=None):
        '''
        Calculate the delta obtained by a SPARQL Update operation.

        This does a couple of extra things:

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
        `BaseStoreLayout.update_resource`.
        '''

        pre_g = self.rdfly.extract_imr().graph

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





