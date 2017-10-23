from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr, transactional, must_exist, \
        ResourceNotExistsError
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
    @must_exist
    def patch(self, data):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH
        '''
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        self.rdfly.patch_rsrc(self.urn, data, ts)

        self.rdfly.ds.add((self.urn, nsc['fcrepo'].lastUpdated, ts))
        self.rdfly.ds.add((self.urn, nsc['fcrepo'].lastUpdatedBy,
                Literal('BypassAdmin')))



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





