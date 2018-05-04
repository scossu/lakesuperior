import hashlib

import lmdb

from lakesuperior import env



class MetadataStore:
    """
    LMDB store for RDF metadata.

    Note that even though this store connector uses LMDB as the
    :py::class:`LmdbStore` class, it is separate because it is not part of the
    RDFLib store implementation and carries higher-level concepts such as LDP
    resource URIs.
    """

    db_labels = (
        'checksums',
    )
    """
    At the moment only ``checksums`` is implemented. It is a registry of
    LDP resource graphs, indicated in the key by their UID, and their
    cryptographic hashes.
    """

    def __init__(self, create=True):
        """
        Initialize DBs.
        """
        path = env.app_globals.config['ldp_rs']['location']
        if not exists(path) and create is True:
            makedirs(path)

        if getattr(env, 'wsgi_options', False):
            self._workers = env.wsgi_options['workers']
        else:
            self._workers = 1
        logger.info('Max LMDB readers: {}'.format(self._workers))

        self.data_env = lmdb.open(
                path + '/metadata', subdir=False, create=create,
                map_size=1024 ** 3 * 10, max_dbs=len(self.dbs),
                max_spare_txns=self._workers)

        self.dbs = {
                label: self.env.open_db(label.encode('ascii'), create=create)
                for label in db_labels}
