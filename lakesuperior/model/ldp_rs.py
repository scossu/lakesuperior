from copy import deepcopy

from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.term import URIRef, Literal, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.model.ldpr import Ldpr, transactional, must_exist, \
        ResourceNotExistsError, ServerManagedTermError
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
            raise ResourceNotExistsError()

        return Translator.globalize_rsrc(im_rsrc)


    @transactional
    def post(self, data, format='text/turtle'):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

        Perform a POST action after a valid resource URI has been found.
        '''
        g = Graph().parse(data=data, format=format, publicID=self.urn)

        self._check_mgd_terms_rdf(g)
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

        self._check_mgd_terms_rdf(g)
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
        self._check_mgd_terms_sparql(data)
        self._ensure_single_subject_sparql_update(data)

        self.rdfly.patch_rsrc(data)


    ## PROTECTED METHODS ##

    def _check_mgd_terms_rdf(self, g):
        '''
        Check whether server-managed terms are in a RDF payload.
        '''
        offending_subjects = set(g.subjects()) & srv_mgd_subjects
        if offending_subjects:
            raise ServerManagedTermError('Some subjects in RDF payload '
                    'are server managed and cannot be modified: {}'
                    .format(' , '.join(offending_subjects)))

        offending_predicates = set(g.predicates()) & srv_mgd_predicates
        if offending_predicates:
            raise ServerManagedTermError('Some predicates in RDF payload '
                    'are server managed and cannot be modified: {}'
                    .format(' , '.join(offending_predicates)))

        offending_types = set(g.objects(predicate=RDF.type)) & srv_mgd_types
        if offending_types:
            raise ServerManagedTermError('Some RDF types in RDF payload '
                    'are server managed and cannot be modified: {}'
                    .format(' , '.join(offending_types)))


    def _check_mgd_terms_sparql(self, q):
        '''
        Parse tokens in update query and verify that none of the terms being
        modified is server-managed.

        The only reasonable way to do this is to perform the query on a copy
        and verify if any of the server managed terms is in the delta. If it
        is, it means that some server-managed term is being modified and
        an error should be raised.

        NOTE: This only checks if a server-managed term is effectively being
        modified. If a server-managed term is present in the query but does not
        cause any change in the updated resource, no error is raised.
        '''

        before_test = self.rdfly.extract_imr().graph
        after_test = deepcopy(before_test)

        after_test.update(q)

        delta = before_test ^ after_test
        self._logger.info('Delta: {}'.format(delta.serialize(format='turtle')
                .decode('utf8')))

        for s,p,o in delta:
            if s in srv_mgd_subjects:
                raise ServerManagedTermError(
                        'Subject {} is server managed and cannot be modified.'
                        .format(s))
            if p in srv_mgd_predicates:
                raise ServerManagedTermError(
                        'Predicate {} is server managed and cannot be modified.'
                        .format(p))
            if p == RDF.type and o in srv_mgd_types:
                raise ServerManagedTermError(
                        'RDF type {} is server managed and cannot be modified.'
                        .format(o))


    def _ensure_single_subject_sparql_update(self, qs):
        '''
        Ensure that a SPARQL update query only affects the current resource.

        This prevents a query such as

        DELETE {
          ?s a ns:Class .
        }
        INSERT {
          ?s a ns:OtherClass .
        }
        WHERE {
          ?s a ns:Class .
        }

        from affecting multiple resources.
        '''
        # @TODO This requires some quirky algebra parsing and manipulation.
        # Will need to investigate.
        pass


    def _ensure_single_subject_rdf(self, g):
        '''
        Ensure that a RDF payload for a POST or PUT has a single resource.
        '''
        if not all(s == self.uri for s in set(g.subjects())):
            return SingleSubjectError(self.uri)


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





