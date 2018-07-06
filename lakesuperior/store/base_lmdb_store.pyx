# cython: language_level = 3

import logging

from contextlib import contextmanager
from os import makedirs, path

from lakesuperior import env

from lakesuperior.cy_include cimport cylmdb as lmdb

from libc cimport errno
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free


logger = logging.getLogger(__name__)


# Global cdefs.

class LmdbError(Exception):
    pass


cdef class BaseLmdbStore:
    """
    Generic LMDB store abstract class.

    This class contains convenience method to create an LMDB store for any
    purpose and provides some convenience methods to wrap cursors and
    transactions into contexts.

    Example usage::

        >>> class MyStore(BaseLmdbStore):
        ...     path = '/base/store/path'
        ...     dbi_flags = ('db1', 'db2')
        ...
        >>> ms = MyStore()
        >>> # "with" wraps the operation in a transaction.
        >>> with ms.cur(index='db1', write=True):
        ...     cur.put(b'key1', b'val1')
        True

    """

    dbi_labels = None
    dbi_flags = None
    """
    Configuration of databases in the environment.

    This is an OderedDict whose keys are the database labels and whose values
    are LMDB flags for creating and opening the databases as per
    `http://www.lmdb.tech/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a`_
    .

    If the environment has only one database, do not override this value (i.e.
    leave it to ``None``).

    :rtype: dict or None
    """

    flags = 0
    """
    LMDB environment flags.

    These are used with ``mdb_env_open``.
    """

    options = {}
    """
    LMDB environment option overrides. Setting this is not required.

    See `LMDB documentation
    <http://lmdb.readthedocs.io/en/release/#environment-class`>_ for details
    on available options.

    Default values are available for the following options:

    - ``map_size``: 1 Gib
    - ``max_dbs``: dependent on the number of DBs defined in
      :py:meth:``dbi_flags``. Only override if necessary.
    - ``max_spare_txns``: dependent on the number of threads, if accessed via
      WSGI, or ``1`` otherwise. Only override if necessary.

    :rtype: dict
    """

    def __cinit__(self, dbpath, create=True):
        """
        Initialize DB environment and databases.
        """
        self.dbpath = dbpath.encode()
        logger.info('Init DB with path: {}'.format(self.dbpath.decode()))

        parent_path = (
                path.dirname(dbpath) if lmdb.MDB_NOSUBDIR & self.flags
                else dbpath)

        if not path.exists(parent_path) and True:
            logger.info(
                    'Creating database directory at {}'.format(parent_path))
            try:
                makedirs(parent_path, mode=0o750, exist_ok=True)
            except Exception as e:
                raise IOError(
                    'Could not create the database at {}. Error: {}'.format(
                        dbpath, e))

        self._open_env()

        self._init_dbis(create)


    def __dealloc__(self):
        PyMem_Free(self.dbis)


    def _open_env(self):
        """
        Create and open database environment.
        """
        # Create environment handle.
        rc = lmdb.mdb_env_create(&self.dbenv)
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError('Error creating DB environment: {}'.format(
                    lmdb.mdb_strerror(rc)))

        # Set map size.
        rc = lmdb.mdb_env_set_mapsize(self.dbenv, self.options.get(
                'map_size', 1024 ** 3))
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError('Error setting DB map size: {}'.format(
                lmdb.mdb_strerror(rc)))

        # Set max databases.
        max_dbs = self.options.get('max_dbs', len(self.dbi_labels))
        rc = lmdb.mdb_env_set_maxdbs(self.dbenv, max_dbs)
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError('Error setting max databases: {}'.format(
                lmdb.mdb_strerror(rc)))

        # Set max readers.
        self.readers = self.options.get('max_spare_txns', False)
        if not self.readers:
            self.readers = (
                    env.wsgi_options['workers']
                    if getattr(env, 'wsgi_options', False)
                    else 1)
            logger.info('Max LMDB readers: {}'.format(self.readers))
        rc = lmdb.mdb_env_set_maxreaders(self.dbenv, self.readers)
        logger.debug('Max. readers: {}'.format(self.readers))
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError('Error setting max readers: {}'.format(
                lmdb.mdb_strerror(rc)))

        # Open DB environment.
        rc = lmdb.mdb_env_open(
                self.dbenv, self.dbpath, self.flags, 0o640)
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError('Error opening the database: {}.'.format(
                lmdb.mdb_strerror(rc)))


    cdef void _init_dbis(self, create=False):
        """
        Initialize databases.
        """
        cdef lmdb.MDB_txn *txn

        self.dbis = <lmdb.MDB_dbi *>PyMem_Malloc(
                len(self.dbi_labels) * sizeof(lmdb.MDB_dbi))

        create_flag = lmdb.MDB_CREATE if create is True else 0
        txn_flags = 0 if create else lmdb.MDB_RDONLY
        rc = lmdb.mdb_txn_begin(self.dbenv, NULL, txn_flags, &txn)
        try:
            for dbidx, dbname in enumerate(self.dbi_labels):
                dbbytename = dbname.encode()
                flags = self.dbi_flags.get(dbname, 0) | create_flag
                logger.debug(
                    'Creating DB {} at index {} and with flags: {}'.format(
                    dbname, dbidx, flags))
                rc = lmdb.mdb_dbi_open(
                        txn, dbbytename,
                        flags,
                        &self.dbis[dbidx])
                logger.debug('Created DB: {}: {}'.format(dbname, rc))
                if rc != lmdb.MDB_SUCCESS:
                    raise LmdbError('Error opening database: {}'.format(
                        lmdb.mdb_strerror(rc)))

            lmdb.mdb_txn_commit(txn)
        except:
            lmdb.mdb_txn_abort(txn)
            raise


    cdef lmdb.MDB_dbi *get_dbi(self, char *dbname):
        """
        Return a DBI pointer by database name.
        """
        cdef lmdb.MDB_dbi *dbi

        dbi = (
                NULL if dbname is None
                else &self.dbis[self.dbconfig.index(dbname)])

        return dbi


    @contextmanager
    def txn_ctx(self, write=False):
        """
        Transaction context manager.

        :param bool write: Whether a write transaction is to be opened.

        :rtype: lmdb.Transaction
        """
        try:
            self.txn = self._txn_begin(write=write)
            yield <object>self.txn
            self._txn_commit()
        except:
            self._txn_abort()
            raise


    @contextmanager
    def cur_ctx(self, dbname=None, txn=None, write=False):
        """
        Handle a cursor on a database by its index as a context manager.

        An existing transaction can be used, otherwise a new one will be
        automatically opened and closed within the cursor context.

        :param str index: The database index. If not specified, a cursor is
            opened for the main database environment.
        :param lmdb.Transaction txn: Existing transaction to use. If not
            specified, a new transaction will be opened.
        :param bool write: Whether a write transaction is to be opened. Only
            meaningful if ``txn`` is ``None``.

        :rtype: lmdb.Cursor
        """
        cdef:
            lmdb.MDB_cursor *cur

        if self.txn is NULL:
            _txn_is_tmp = True
            self.txn = self._txn_begin(write=write)
        else:
            _txn_is_tmp = False

        try:
            cur = self._cur_open(<lmdb.MDB_txn *>txn, dbname)
            yield <object>cur
            self._cur_close(cur)
            if _txn_is_tmp is True:
                self._txn_commit()
        except:
            if _txn_is_tmp is True:
                self._txn_abort()
            raise
        finally:
            if _txn_is_tmp is True:
                self.txn = NULL


    cdef lmdb.MDB_cursor *_cur_open(self, lmdb.MDB_txn *txn, char *dbname=NULL):
        cdef:
            lmdb.MDB_cursor *cur

        rc = lmdb.mdb_cursor_open(txn, self.get_dbi(dbname)[0], &cur)
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError(
                    'Error opening cursor: {}'.format(lmdb.mdb_strerror(rc)))

        return cur


    cdef void _cur_close(self, lmdb.MDB_cursor *cur):
        pass


    cdef lmdb.MDB_txn *_txn_begin(self, write=True, lmdb.MDB_txn *parent=NULL):
        cdef:
            lmdb.MDB_txn *txn
            unsigned int flags

        flags = 0 if write else lmdb.MDB_RDONLY

        rc = lmdb.mdb_txn_begin(self.dbenv, parent, 0, &txn)
        if rc != lmdb.MDB_SUCCESS:
            raise LmdbError(
                'Unknown error code opening transaction: {}'.format(
                    lmdb.mdb_strerror(rc)))

        return txn
