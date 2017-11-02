from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.resource import Resource
from rdflib.term import URIRef, Literal, Variable

from lakesuperior.config_parser import config
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr, transactional, must_exist
from lakesuperior.util.digest import Digest

class LdpNr(Ldpr):
    '''LDP-NR (Non-RDF Source).

    Definition: https://www.w3.org/TR/ldp/#ldpnr
    '''

    base_types = {
        nsc['fcrepo'].Binary,
        nsc['ldp'].NonRDFSource,
    }


    ## LDP METHODS ##

    def get(self, *args, **kwargs):
        raise NotImplementedError()


    def post(self, data):
        #self._logger.debug('Data: {}'.format(data[:256]))
        metadata_rsrc = Resource(Graph(), self.urn)

        for t in self.base_types:
            metadata_rsrc.add(RDF.type, t)

        cksum = Digest.non_rdf_cksum(data)
        cksum_term = URIRef('urn:sha1:{}'.format(cksum))
        metadata_rsrc.add(nsc['premis'].hasMessageDigest, cksum_term)

        self._store_binary(data, cksum)



    def put(self, data):
        raise NotImplementedError()



    ## PROTECTED METHODS ##

    def _store_binary(self, data, cksum):
        '''
        Move a binary file to persistent storage.

        @param data (bytes) Binary data to store.
        @param cksum (string) Digest of the data. This is used to determine the
        file location.
        '''
        pass

