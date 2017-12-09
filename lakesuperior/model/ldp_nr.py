from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.resource import Resource
from rdflib.term import URIRef, Literal, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr, atomic
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

    def __init__(self, uuid, stream=None, mimetype='application/octet-stream',
            disposition=None, **kwargs):
        '''
        Extends Ldpr.__init__ by adding LDP-NR specific parameters.
        '''
        super().__init__(uuid, **kwargs)

        if stream:
            self.workflow = self.WRKF_INBOUND
            self.stream = stream
        else:
            self._imr_options = {}
            self.workflow = self.WRKF_OUTBOUND

        self.mimetype = mimetype
        self.disposition = disposition


    @property
    def filename(self):
        return self.imr.value(nsc['ebucore'].filename)


    @property
    def local_path(self):
        cksum_term = self.imr.value(nsc['premis'].hasMessageDigest)
        cksum = str(cksum_term.identifier.replace('urn:sha1:',''))
        return self.nonrdfly.local_path(cksum)


    ## LDP METHODS ##

    @atomic
    def _create_or_replace_rsrc(self, create_only=False):
        '''
        Create a new binary resource with a corresponding RDF representation.

        @param file (Stream) A Stream resource representing the uploaded file.
        '''
        # Persist the stream.
        file_uuid = self.digest = self.nonrdfly.persist(self.stream)

        # Try to persist metadata. If it fails, delete the file.
        self._logger.debug('Persisting LDP-NR triples in {}'.format(self.urn))
        try:
            ev_type = super()._create_or_replace_rsrc(create_only)
        except:
            self.nonrdfly.delete(file_uuid)
            raise
        else:
            return ev_type


    ## PROTECTED METHODS ##

    def _add_srv_mgd_triples(self, create=False):
        '''
        Add all metadata for the RDF representation of the LDP-NR.

        @param stream (BufferedIO) The uploaded data stream.
        @param mimetype (string) MIME type of the uploaded file.
        @param disposition (defaultdict) The `Content-Disposition` header
        content, parsed through `parse_rfc7240`.
        '''
        super()._add_srv_mgd_triples(create)

        # File size.
        self._logger.debug('Data stream size: {}'.format(self.stream.limit))
        self.provided_imr.set(nsc['premis'].hasSize,
                Literal(self.stream.limit))

        # Checksum.
        cksum_term = URIRef('urn:sha1:{}'.format(self.digest))
        self.provided_imr.set(nsc['premis'].hasMessageDigest, cksum_term)

        # MIME type.
        self.provided_imr.set(nsc['ebucore']['hasMimeType'], 
                Literal(self.mimetype))

        # File name.
        self._logger.debug('Disposition: {}'.format(self.disposition))
        try:
            self.provided_imr.set(nsc['ebucore']['filename'], Literal(
                    self.disposition['attachment']['parameters']['filename']))
        except (KeyError, TypeError) as e:
            pass
