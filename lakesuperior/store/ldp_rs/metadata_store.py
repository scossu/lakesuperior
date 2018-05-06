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

    db_labels = ('checksums',)
    """
    At the moment only ``checksums`` is implemented. It is a registry of
    LDP resource graphs, indicated in the key by their UID, and their
    cryptographic hashes.
    """

    path = path.join(
        env.app_globals.config['application']['store']['ldp_rs']['location'],
        'metadata')


    def get_checksum(self, uri):
        """
        Get the checksum of a resource.

        :param str uri: Resource URI (``info:fcres...``).
        :rtype: bytes
        """
        with self.cur(index='checksums') as cur:
            cksum = cur.get(uri.encode('utf-8'))

        return cksum


    def update_checksum(self, uri, cksum):
        """
        Update the stored checksum of a resource.

        :param str uri: Resource URI (``info:fcres...``).
        :param bytes cksum: Checksum bytestring.
        """
        with self.cur(index='checksums', write=True) as cur:
            cur.put(uri.encode('utf-8'), cksum)


    def delete_checksum(self, uri):
        """
        Delete the stored checksum of a resource.

        :param str uri: Resource URI (``info:fcres...``).
        """
        with self.cur(index='checksums', write=True) as cur:
            if cur.set_key(uri.encode('utf-8')):
                cur.delete()
