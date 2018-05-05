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
