from os import path

from lakesuperior.store.base_lmdb_store import BaseLmdbStore

from lakesuperior import env


class MetadataStore(BaseLmdbStore):
    """
    LMDB store for RDF metadata.

    Note that even though this store connector uses LMDB as the
    :py::class:`LmdbStore` class, it is separate because it is not part of the
    RDFLib store implementation and carries higher-level concepts such as LDP
    resource URIs.
    """

    dbi_labels = [
        'checksums',
        'event_queue'
    ]
    """
    Currently implemented:

    - ``checksums``: registry of LDP resource graphs, indicated in the key by
      their UID, and their cryptographic hashes.

    """


    def get_checksum(self, uri):
        """
        Get the checksum of a resource.

        :param str uri: Resource URI (``info:fcres...``).
        :rtype: bytes
        """
        with self.txn_ctx():
            return self.get_data(uri.encode(), 'checksums')


    def update_checksum(self, uri, cksum):
        """
        Update the stored checksum of a resource.

        :param str uri: Resource URI (``info:fcres...``).
        :param bytes cksum: Checksum bytestring.
        """
        with self.txn_ctx(True):
            self.put(uri.encode(), cksum, 'checksums')


    def delete_checksum(self, uri):
        """
        Delete the stored checksum of a resource.

        :param str uri: Resource URI (``info:fcres...``).
        """
        with self.txn_ctx(True):
            self.delete(uri.encode(), 'checksums')
