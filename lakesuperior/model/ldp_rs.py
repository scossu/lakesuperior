from copy import deepcopy

from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.term import URIRef, Literal, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.model.ldpr import Ldpr, transactional, must_exist
from lakesuperior.exceptions import ResourceNotExistsError, \
        ServerManagedTermError, SingleSubjectError
from lakesuperior.util.translator import Translator

class LdpRs(Ldpr):
    '''LDP-RS (LDP RDF source).

    Definition: https://www.w3.org/TR/ldp/#ldprs
    '''
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


    def head(self):
        '''
        Return values for the headers.
        '''
        headers = self.rdfly.headers

        for t in self.ldp_types:
            headers['Link'].append('{};rel="type"'.format(t.identifier.n3()))

        return headers


    def get(self, inbound=False, children=True, srv_mgd=True):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_GET
        '''
        im_rsrc = self.rdfly.out_rsrc(inbound)
        if not len(im_rsrc.graph):
            raise ResourceNotExistsError(im_rsrc.uuid)

        return Translator.globalize_rsrc(im_rsrc)


    @transactional
    def post(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

        Perform a POST action after a valid resource URI has been found.
        '''
        g = Graph().parse(data=data, format=format, publicID=self.urn)

        self._check_mgd_terms(g)
        self._ensure_single_subject_rdf(g)

        for t in self.base_types:
            g.add((self.urn, RDF.type, t))

        self.rdfly.create_rsrc(g)

        self._set_containment_rel()


    @transactional
    def put(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT
        '''
        g = Graph().parse(data=data, format=format, publicID=self.urn)

        self._check_mgd_terms(g)
        self._ensure_single_subject_rdf(g)

        for t in self.base_types:
            g.add((self.urn, RDF.type, t))

        self.rdfly.create_or_replace_rsrc(g)

        self._set_containment_rel()


    @transactional
    @must_exist
    def patch(self, data):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH
        '''
        remove, add = self._sparql_delta(data)

        self.rdfly.modify_rsrc(remove, add)


    ## PROTECTED METHODS ##

    def _check_mgd_terms(self, g):
        '''
        Check whether server-managed terms are in a RDF payload.
        '''
        offending_subjects = set(g.subjects()) & srv_mgd_subjects
        if offending_subjects:
            raise ServerManagedTermError(offending_subjects, 's')

        offending_predicates = set(g.predicates()) & srv_mgd_predicates
        if offending_predicates:
            raise ServerManagedTermError(offending_predicates, 'p')

        offending_types = set(g.objects(predicate=RDF.type)) & srv_mgd_types
        if offending_types:
            raise ServerManagedTermError(offending_types, 't')


    def _sparql_delta(self, q):
        '''
        Calculate the delta obtained by a SPARQL Update operation.

        This does a couple of extra things:

        1. It ensures that no resources outside of the subject of the request
        are modified (e.g. by variable subjects)
        2. It verifies that none of the terms being modified is server-managed.

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

        self._check_mgd_terms(remove + add)

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





