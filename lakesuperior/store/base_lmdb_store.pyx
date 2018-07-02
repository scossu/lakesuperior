# distutils: include_dirs = ../include
# distutils: library_dirs = ../lib, ../include
# cython: language_level = 3

import hashlib
import logging

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from os import makedirs, path

from lakesuperior import env

cimport lmdb

from cpython.ref cimport PyObject
from libc cimport errno


logger = logging.getLogger(__name__)


# Global cdefs.

cdef:
    int rc
    size_t i


class LmdbError(Exception):
    pass


cdef class BaseLmdbStore:
    """
    Generic LMDB store abstract class.

    This class contains convenience method to create an LMDB store for any
    purpose and provides some convenience methods to wrap cursors and
    transactions into contexts.

    This interface can be subclassed for specific storage back ends. It is
    *not* used for :py:class:`~lakesuperior.store.ldp_rs.lmdb_store.LmdbStore`
    which has a more complex lifecycle and setup.

    Example usage::

        >>> class MyStore(BaseLmdbStore):
        ...     path = '/base/store/path'
        ...     db_labels = ('db1', 'db2')
        ...
        >>> ms = MyStore()
        >>> # "with" wraps the operation in a transaction.
        >>> with ms.cur(index='db1', write=True):
        ...     cur.put(b'key1', b'val1')
        True

    """

    cdef:
        lmdb.MDB_env *dbenv
        lmdb.MDB_txn *txn
        unsigned int readers

    path = None
    """
    Filesystem path where the database environment is stored.

    This is a mandatory value for implementations.

    :rtype: str
    """

    db_labels = None
    """
    List of databases in the DB environment by label.

    If the environment has only one database, do not override this value (i.e.
    leave it to ``None``).

    :rtype: tuple(str)
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
      :py:meth:``db_labels``. Only override if necessary.
    - ``max_spare_txns``: dependent on the number of threads, if accessed via
      WSGI, or ``1`` otherwise. Only override if necessary.

    :rtype: dict
    """

    def __init__(self, create=True):
        """
        Initialize DB environment and databases.
        """
        if not path.exists(self.path) and create is True:
            try:
                makedirs(self.path)
            except Exception as e:
                raise IOError(
                    'Could not create the database at {}. Error: {}'.format(
                        self.path, e))

        self._open_env()

        if self.db_labels is not None:
            self._dbs = {
                label: self._dbenv.open_db(
                    label.encode('ascii'), create=create)
                for label in self.db_labels}


    def _open_env(self):
        """
        Open database environment.
        """
        max_dbs = self.options.get('max_dbs', len(self.db_labels))

        rc = lmdb.mdb_env_create(&self.dbenv)
        assert (
                rc == lmdb.MDB_SUCCESS,
                'Unknown error code creating DB environment: {}'.format(rc))

        rc = lmdb.mdb_env_set_mapsize(self.dbenv, self.options.get(
                'map_size', 1024 ** 2))

        rc = lmdb.mdb_env_set_maxdbs(self.dbenv, max_dbs)

        self.readers = self.options.get('max_spare_txns', False)
        if &self.readers is NULL:
            self.readers = (
                    env.wsgi_options['workers']
                    if getattr(env, 'wsgi_options', False)
                    else 1)
            logger.info('Max LMDB readers: {}'.format(self.readers))
        rc = lmdb.mdb_env_set_maxreaders(self.dbenv, self.readers)

        rc = lmdb.mdb_env_open(self.dbenv, self.path, self.flags, 0o644)
        if rc == lmdb.MDB_VERSION_MISMATCH:
            raise LmdbError('LMDB version mismatch.')
        elif rc == lmdb.MDB_INVALID:
            raise LmdbError('Environment file headers are corrupted.')
        elif rc == errno.ENOENT:
            raise LmdbError('No directory under this path.')
        elif rc == errno.EACCES:
            raise LmdbError(
                    'Insufficient permissions to open the database file.')
        elif rc == errno.EAGAIN:
            raise LmdbError('The database has been locked by another process.')
        elif rc != lmdb.MDB_SUCCESS:
            raise LmdbError(
                'Unknown error code opening the database: {}.'.format(rc))



    @property
    def dbs(self):
        """
        List of databases in the environment, as LMDB handles.

        These handles can be used to begin transactions.

        :rtype: tuple
        """
        return self._dbs


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
    def cur_ctx(self, index=None, txn=None, write=False):
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
            lmdb.MDB_txn *tmp_txn

        db = None if index is None else self.dbs[index]

        if self.txn is NULL:
            _txn_is_tmp = True
            self.txn = self._txn_begin(write=write)
        else:
            _txn_is_tmp = False

        try:
            cur = self._cur_open(<lmdb.MDB_txn *>txn, db)
            yield <object>cur
            self._cur_close(cur)
            if _txn_is_tmp:
                self._txn_commit()
        except:
            if _txn_is_tmp:
                self._txn_abort()
            raise


    cdef lmdb.MDB_cursor *_cur_open(
            self, lmdb.MDB_txn *txn, str dbname, unsigned int flags=0):
        cdef:
            lmdb.MDB_cursor *cur
            lmdb.MDB_dbi dbi

        dbi = self.dbis[dbname]
        rc = lmdb.mdb_dbi_open(txn, dbname, flags, &dbi)
        if rc == lmdb.MDB_NOTFOUND:
            raise LmdbError('No database found with label "{}"'.format(dbname))
        elif rc == lmdb.MDB_DBS_FULL:
            raise LmdbError('Max. database limit reached.'.format(dbname))
        assert rc == lmdb.MDB_SUCCESS, 'Error opening database: {}'.format(rc)

        rc = lmdb.mdb_cursor_open(txn, dbi, &cur)
        if rc == errno.EINVAL:
            raise ValueError('Invalid parameter used to open cursor.')
        assert (
            rc == lmdb.MDB_SUCCESS,
            'Unknown error code opening cursor: {}'.format(rc))

        return cur


    cdef void _cur_close(self, lmdb.MDB_cursor *cur):
        pass


    cdef lmdb.MDB_txn *_txn_begin(self, write=True, lmdb.MDB_txn *parent=NULL):
        cdef:
            lmdb.MDB_txn *txn
            unsigned int flags

        flags = 0 if write else lmdb.MDB_RDONLY

        rc = lmdb.mdb_txn_begin(self.dbenv, parent, 0, &txn)
        if rc == lmdb.MDB_PANIC:
            raise LmdbError('Fatal error opening transaction.')
        elif rc == lmdb.MDB_MAP_RESIZED:
            raise LmdbError('Size boundary of memory map exceeded.')
        elif rc == lmdb.MDB_READERS_FULL:
            raise LmdbError('Max readers limit reached.')
        elif rc == errno.ENOMEM:
            raise LmdbError('Max readers limit reached.')
        assert (
            rc == lmdb.MDB_SUCCESS,
            'Unknown error code opening transaction: {}'.format(rc))

        return txn
