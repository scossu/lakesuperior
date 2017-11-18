from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.resource import Resource
from rdflib.term import URIRef, Literal, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr, transactional, must_exist
from lakesuperior.model.ldp_rs import LdpRs

class LdpNr(Ldpr):
    '''LDP-NR (Non-RDF Source).

    Definition: https://www.w3.org/TR/ldp/#ldpnr
    '''

    base_types = {
        nsc['fcrepo'].Binary,
        nsc['fcrepo'].Resource,
        nsc['ldp'].Resource,
        nsc['ldp'].NonRDFSource,
    }


    @property
    def nonrdfly(self):
        '''
        Load non-RDF (binary) store layout.
        '''
        if not hasattr(self, '_nonrdfly'):
            self._nonrdfly = __class__.load_layout('non_rdf')

        return self._nonrdfly


    @property
    def filename(self):
        return self.imr.value(nsc['ebucore'].filename)


    @property
    def local_path(self):
        cksum_term = self.imr.value(nsc['premis'].hasMessageDigest)
        cksum = str(cksum_term.identifier.replace('urn:sha1:',''))
        return self.nonrdfly.local_path(cksum)


    ## LDP METHODS ##

    def get(self, **kwargs):
        return LdpRs(self.uuid).get(**kwargs)


    @transactional
    def post(self, stream, mimetype=None, disposition=None):
        '''
        Create a new binary resource with a corresponding RDF representation.

        @param file (Stream) A Stream resource representing the uploaded file.
        '''
        # Persist the stream.
        uuid = self.nonrdfly.persist(stream)

        # Gather RDF metadata.
        self._add_metadata(stream, mimetype=mimetype, disposition=disposition)

        # Try to persist metadata. If it fails, delete the file.
        self._logger.debug('Persisting LDP-NR triples in {}'.format(
            self.urn))
        try:
            rsrc = self.rdfly.create_rsrc(self.imr)
        except:
            self.nonrdfly.delete(uuid)
        else:
            return rsrc


    def put(self, stream, **kwargs):
        return self.post(stream, **kwargs)


    ## PROTECTED METHODS ##

    def _add_metadata(self, stream, mimetype='application/octet-stream',
            disposition=None):
        '''
        Add all metadata for the RDF representation of the LDP-NR.

        @param stream (BufferedIO) The uploaded data stream.
        @param mimetype (string) MIME type of the uploaded file.
        @param disposition (defaultdict) The `Content-Disposition` header
        content, parsed through `parse_rfc7240`.
        '''
        # File size.
        self._logger.debug('Data stream size: {}'.format(stream.limit))
        self.stored_or_new_imr.add(nsc['premis'].hasSize, Literal(stream.limit,
                datatype=XSD.long))

        # Checksum.
        cksum_term = URIRef('urn:sha1:{}'.format(self.uuid))
        self.imr.add(nsc['premis'].hasMessageDigest, cksum_term)

        # MIME type.
        self.imr.add(nsc['ebucore']['hasMimeType'], Literal(
                mimetype, datatype=XSD.string))

        # File name.
        self._logger.debug('Disposition: {}'.format(disposition))
        try:
            self.imr.add(nsc['ebucore']['filename'], Literal(
                    disposition['attachment']['parameters']['filename'],
                    datatype=XSD.string))
        except KeyError:
            pass
