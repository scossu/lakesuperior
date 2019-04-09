import logging
import pdb

from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.resource import Resource
from rdflib.term import URIRef, Literal, Variable

from lakesuperior import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldp.ldpr import Ldpr
from lakesuperior.model.ldp.ldp_rs import LdpRs


nonrdfly = env.app_globals.nonrdfly
logger = logging.getLogger(__name__)
default_hash_algo = env.app_globals.config['application']['uuid']['algo']

class LdpNr(Ldpr):
    """LDP-NR (Non-RDF Source).

    Definition: https://www.w3.org/TR/ldp/#ldpnr
    """

    base_types = {
        nsc['fcrepo'].Binary,
        nsc['fcrepo'].Resource,
        nsc['ldp'].Resource,
        nsc['ldp'].NonRDFSource,
    }

    def __init__(self, uuid, stream=None, mimetype=None,
            disposition=None, prov_cksum_algo=None, prov_cksum=None,
            **kwargs):
        """
        Extends Ldpr.__init__ by adding LDP-NR specific parameters.
        """
        super().__init__(uuid, **kwargs)

        self._imr_options = {}
        if stream:
            self.workflow = self.WRKF_INBOUND
            self.stream = stream
        else:
            self.workflow = self.WRKF_OUTBOUND

        if mimetype:
            self.mimetype = mimetype
        else:
            self.mimetype = (
                    str(self.metadata.value(nsc['ebucore'].hasMimeType))
                    if self.is_stored
                    else 'application/octet-stream')

        self.prov_cksum_algo = prov_cksum_algo
        self.prov_cksum = prov_cksum
        self.disposition = disposition


    @property
    def filename(self):
        """
        File name of the original uploaded file.

        :rtype: str
        """
        return self.metadata.value(nsc['ebucore'].filename)


    @property
    def content(self):
        """
        Binary content.

        :return: File handle of the resource content.
        :rtype: io.BufferedReader
        """
        return open(self.local_path, 'rb')


    @property
    def content_size(self):
        """
        Byte size of the binary content.

        :rtype: int
        """
        return int(self.metadata.value(nsc['premis'].hasSize))


    @property
    def local_path(self):
        """
        Path on disk of the binary content.

        :rtype: str
        """
        cksum_term = self.metadata.value(nsc['premis'].hasMessageDigest)
        cksum = str(cksum_term).replace(f'urn:{default_hash_algo}:','')
        return nonrdfly.__class__.local_path(
                nonrdfly.root, cksum, nonrdfly.bl, nonrdfly.bc)


    def create_or_replace(self, create_only=False):
        """
        Create a new binary resource with a corresponding RDF representation.

        :param bool create_only: Whether the resource is being created or
            updated.
        """
        # Persist the stream.
        self.digest, self.size = nonrdfly.persist(
                self.uid, self.stream, prov_cksum_algo=self.prov_cksum_algo,
                prov_cksum=self.prov_cksum)

        # Try to persist metadata. If it fails, delete the file.
        logger.debug('Persisting LDP-NR triples in {}'.format(self.uri))
        try:
            ev_type = super().create_or_replace(create_only)
        except:
            # self.digest is also the file UID.
            nonrdfly.delete(self.digest)
            raise
        else:
            return ev_type


    ## PROTECTED METHODS ##

    def _add_srv_mgd_triples(self, *args, **kwargs):
        """
        Add all metadata for the RDF representation of the LDP-NR.

        :param BufferedIO stream: The uploaded data stream.
        :param str mimetype: MIME type of the uploaded file.
        :param defaultdict disposition: The ``Content-Disposition`` header
            content, parsed through ``parse_rfc7240``.
        """
        super()._add_srv_mgd_triples(*args, **kwargs)

        # File size.
        logger.debug('Data stream size: {}'.format(self.size))
        self.provided_imr.set((
            self.uri, nsc['premis'].hasSize, Literal(self.size)))

        # Checksum.
        cksum_term = URIRef(f'urn:{default_hash_algo}:{self.digest}')
        self.provided_imr.set((
            self.uri, nsc['premis'].hasMessageDigest, cksum_term))

        # MIME type.
        self.provided_imr.set((
            self.uri, nsc['ebucore']['hasMimeType'], Literal(self.mimetype)))

        # File name.
        logger.debug('Disposition: {}'.format(self.disposition))
        try:
            self.provided_imr.set((
                self.uri, nsc['ebucore']['filename'], Literal(
                self.disposition['attachment']['parameters']['filename'])))
        except (KeyError, TypeError) as e:
            pass
