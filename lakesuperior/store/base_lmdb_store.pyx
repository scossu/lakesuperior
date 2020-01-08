import logging
import os
import threading
import multiprocessing

from contextlib import contextmanager
from os import makedirs, path
from shutil import rmtree

from lakesuperior import env, wsgi

from lakesuperior.cy_include cimport cylmdb as lmdb

from libc cimport errno
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cython.parallel import parallel, prange


logger = logging.getLogger(__name__)


cdef void _check(int rc, str message='') except *:
    """
    Check return code.
    """
    if rc == lmdb.MDB_NOTFOUND:
        raise KeyNotFoundError()
    if rc == lmdb.MDB_KEYEXIST:
        raise KeyExistsError()
    if rc == errno.EINVAL:
        raise InvalidParamError(
            'Invalid LMDB parameter error.\n'
            'Please verify that a transaction is open and valid for the '
            'current operation.'
        )
    if rc != lmdb.MDB_SUCCESS:
        out_msg = (
                message + '\nInternal error ({}): '.format(rc)
                if len(message) else 'LMDB Error ({}): '.format(rc))
        out_msg += lmdb.mdb_strerror(rc).decode()
        raise LmdbError(out_msg)


class LmdbError(Exception):
    pass

class KeyNotFoundError(LmdbError):
    pass

class KeyExistsError(LmdbError):
    pass

class InvalidParamError(LmdbError):
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

    dbi_labels = []
    dbi_flags = {}
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

    env_flags = 0
    """
    LMDB environment flags.

    See `mdb_env_open
    <http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340>`_
    """

    env_perms = 0o640
    """
    The UNIX permissions to set on created environment files and semaphores.

    See `mdb_env_open
    <http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340>`_
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

    readers_mult = 4
    """
    Number to multiply WSGI workers by to set the numer of LMDB reader slots.
    """

    ### INIT & TEARDOWN ###

    def __init__(self, env_path, open_env=True, create=True):
        """
        Initialize DB environment and databases.

        :param str env_path: The file path of the store.
        :param bool open: Whether to open the store immediately. If ``False``
            the store can be manually opened with :py:meth:`opn_env`.
        :param bool create: Whether the file and directory structure should
            be created if the store is opened immediately.
        """
        self._open = False
        self.is_txn_open = False
        self.env_path = env_path
        if open_env:
            self.open_env(create)
        #logger.info('Init DB with path: {}'.format(self.env_path))


    def __dealloc__(self):
        self.close_env()


    @property
    def is_open(self):
        return self._open


    @property
    def readers(self):
        return self._readers


    def open_env(self, create):
        """
        Open, and optionally create, store environment.
        """
        if self.is_open:
            logger.warning('Environment already open.')
            return

        logger.debug('Opening environment at {}.'.format(self.env_path))
        if create:
            #logger.info('Creating db env at {}'.format(self.env_path))
            parent_path = (
                    path.dirname(self.env_path)
                    if lmdb.MDB_NOSUBDIR & self.flags
                    else self.env_path)

            if not path.exists(parent_path):
                #logger.info(
                #        'Creating store directory at {}'.format(parent_path))
                try:
                    makedirs(parent_path, mode=0o750, exist_ok=True)
                except Exception as e:
                    raise LmdbError(
                        'Could not create store at {}. Error: {}'.format(
                            self.env_path, e))

        # Create environment handle.
        _check(
                lmdb.mdb_env_create(&self.dbenv),
                'Error creating DB environment handle: {}')
        logger.debug('Created DBenv @ {:x}'.format(<unsigned long>self.dbenv))

        # Set map size.
        _check(
                lmdb.mdb_env_set_mapsize(self.dbenv, self.options.get(
                    'map_size', 1024 ** 3)),
                'Error setting map size: {}')

        # Set max databases.
        max_dbs = self.options.get('max_dbs', len(self.dbi_labels))
        _check(
                lmdb.mdb_env_set_maxdbs(self.dbenv, max_dbs),
                'Error setting max. databases: {}')

        # Set max readers.
        self._readers = self.options.get(
                'max_spare_txns', wsgi.workers * self.readers_mult)
        _check(
                lmdb.mdb_env_set_maxreaders(self.dbenv, self._readers),
                'Error setting max. readers: {}')
        logger.debug('Max. readers: {}'.format(self._readers))

        # Clear stale readers.
        self._clear_stale_readers()

        # Open DB environment.
        logger.debug('DBenv address: {:x}'.format(<unsigned long>self.dbenv))
        _check(
                lmdb.mdb_env_open(
                    self.dbenv, self.env_path.encode(),
                    self.env_flags, self.env_perms),
                f'Error opening the database environment: {self.env_path}.')

        self._init_dbis(create)
        self._open = True


    cdef void _clear_stale_readers(self) except *:
        """
        Clear stale readers.
        """
        cdef int stale_readers

        _check(lmdb.mdb_reader_check(self.dbenv, &stale_readers))
        if stale_readers > 0:
            logger.debug('Cleared {} stale readers.'.format(stale_readers))


    cdef void _init_dbis(self, create=True) except *:
        """
        Initialize databases and cursors.
        """
        cdef:
            size_t i
            lmdb.MDB_txn *txn
            lmdb.MDB_dbi dbi

        # At least one slot (for environments without a database)
        self.dbis = <lmdb.MDB_dbi *>PyMem_Malloc(
                max(len(self.dbi_labels), 1) * sizeof(lmdb.MDB_dbi))
        if not self.dbis:
            raise MemoryError()

        # DBIs seem to start from 2. We want to map cursor pointers in the
        # array to DBIs, so we need an extra slot.
        self.curs = <lmdb.MDB_cursor **>PyMem_Malloc(
                (len(self.dbi_labels) + 2) * sizeof(lmdb.MDB_cursor*))
        if not self.curs:
            raise MemoryError()

        create_flag = lmdb.MDB_CREATE if create is True else 0
        txn_flags = 0 if create else lmdb.MDB_RDONLY
        rc = lmdb.mdb_txn_begin(self.dbenv, NULL, txn_flags, &txn)
        logger.info(f'Creating DBs.')
        try:
            if len(self.dbi_labels):
                for i, dblabel in enumerate(self.dbi_labels):
                    flags = self.dbi_flags.get(dblabel, 0) | create_flag
                    _check(lmdb.mdb_dbi_open(
                            txn, dblabel, flags, self.dbis + i))
                    dbi = self.dbis[i]
                    logger.debug(f'Created DB {dblabel}: {dbi}')
                    # Open and close cursor to initialize the memory slot.
                    _check(lmdb.mdb_cursor_open(
                        txn, dbi, self.curs + dbi))
                    #lmdb.mdb_cursor_close(self.curs[dbi])
            else:
                _check(lmdb.mdb_dbi_open(txn, NULL, 0, self.dbis))
                _check(lmdb.mdb_cursor_open(txn, self.dbis[0], self.curs))
                #lmdb.mdb_cursor_close(self.curs[self.dbis[0]])

            _check(lmdb.mdb_txn_commit(txn))
        except:
            lmdb.mdb_txn_abort(txn)
            raise


    cpdef void close_env(self, bint commit_pending_transaction=False) except *:
        #logger.debug('Cleaning up store env.')
        if self.is_open:
            #logger.debug('Closing store env.')
            if self.is_txn_open is True:
                if commit_pending_transaction:
                    self._txn_commit()
                else:
                    self._txn_abort()

            self._clear_stale_readers()

            PyMem_Free(self.dbis)
            PyMem_Free(self.curs)
            lmdb.mdb_env_close(self.dbenv)

        self._open = False


    cpdef void destroy(self, _path='') except *:
        """
        Destroy the store.

        https://www.youtube.com/watch?v=lIVq7FJnPwg

        :param str _path: unused. Left for RDFLib API compatibility. (actually
            quite dangerous if it were used: it could turn into a
            general-purpose recursive file and folder delete method!)
        """
        if path.exists(self.env_path):
            if lmdb.MDB_NOSUBDIR & self.flags:
                try:
                    os.unlink(self.env_path)
                except FileNotFoundError:
                    pass
                try:
                    os.unlink(self.env_path + '-lock')
                except FileNotFoundError:
                    pass
            else:
                rmtree(self.env_path)


    ### PYTHON-ACCESSIBLE METHODS ###

    @contextmanager
    def txn_ctx(self, write=False):
        """
        Transaction context manager.

        Open and close a transaction for the duration of the functions in the
        context. If a transaction has already been opened in the store, a new
        one is opened only if the current transaction is read-only and the new
        requested transaction is read-write.

        If a new write transaction is opened, the old one is kept on hold until
        the new transaction is closed, then restored. All cursors are
        invalidated and must be restored as well if one needs to reuse them.

        :param bool write: Whether a write transaction is to be opened.

        :rtype: lmdb.Transaction
        """
        cdef lmdb.MDB_txn* hold_txn

        will_open = False

        if not self.is_open:
            raise LmdbError('Store is not open.')

        # If another transaction is open, only open the new transaction if
        # the current one is RO and the new one RW.
        if self.is_txn_open:
            if write:
                will_open = not self.is_txn_rw
        else:
            will_open = True

        # If a new transaction needs to be opened and replace the old one,
        # the old one must be put on hold and swapped out when the new txn
        # is closed.
        if will_open:
            will_reset = self.is_txn_open

        if will_open:
            #logger.debug('Beginning {} transaction.'.format(
            #    'RW' if write else 'RO'))
            if will_reset:
                hold_txn = self.txn

            try:
                self._txn_begin(write=write)
                self.is_txn_rw = write
                #logger.debug('In txn_ctx, before yield')
                yield
                #logger.debug('In txn_ctx, after yield')
                self._txn_commit()
                #logger.debug('after _txn_commit')
                if will_reset:
                    lmdb.mdb_txn_reset(hold_txn)
                    self.txn = hold_txn
                    _check(lmdb.mdb_txn_renew(self.txn))
                    self.is_txn_rw = False
            except:
                self._txn_abort()
                raise
        else:
            logger.info(
                'Transaction is already active. Not opening another one.'
            )
            #logger.debug('before yield')
            yield
            #logger.debug('after yield')


    def begin(self, write=False):
        """
        Begin a transaction manually if not already in a txn context.

        The :py:meth:`txn_ctx` context manager should be used whenever
            possible rather than this method.
        """
        if not self.is_open:
            raise RuntimeError('Store must be opened first.')
        #logger.debug('Beginning a {} transaction.'.format(
        #    'read/write' if write else 'read-only'))

        self._txn_begin(write=write)


    def commit(self):
        """Commit main transaction."""
        #logger.debug('Committing transaction.')
        self._txn_commit()


    def abort(self):
        """Abort main transaction."""
        #logger.debug('Rolling back transaction.')
        self._txn_abort()


    def rollback(self):
        """Alias for :py:meth:`abort`"""
        self.abort()


    def key_exists(self, key, dblabel='', new_txn=True):
        """
        Return whether a key exists in a database (Python-facing method).

        Wrap in a new transaction. Only use this if a transaction has not been
        opened.
        """
        if new_txn is True:
            with self.txn_ctx():
                return self._key_exists(key, len(key), dblabel=dblabel)
        else:
            return self._key_exists(key, len(key), dblabel=dblabel)


    cdef inline bint _key_exists(
            self, unsigned char *key, unsigned char klen,
            DbLabel dblabel=b'') except -1:
        """
        Return whether a key exists in a database.

        To be used within an existing transaction.
        """
        cdef lmdb.MDB_val key_v, data_v

        key_v.mv_data = key
        key_v.mv_size = klen
        #logger.debug(
        #        'Checking if key {} with size {} exists...'.format(key, klen))
        try:
            _check(lmdb.mdb_get(
                self.txn, self.get_dbi(dblabel), &key_v, &data_v))
        except KeyNotFoundError:
            #logger.debug('...no.')
            return False
        #logger.debug('...yes.')
        return True


    def put(self, key, data, dblabel='', flags=0):
        """
        Put one key/value pair (Python-facing method).
        """
        self._put(
            key, len(key), data, len(data), dblabel=dblabel,
            txn=self.txn, flags=flags
        )


    cdef void _put(
            self, unsigned char *key, size_t key_size, unsigned char *data,
            size_t data_size, DbLabel dblabel='',
            lmdb.MDB_txn *txn=NULL, unsigned int flags=0) except *:
        """
        Put one key/value pair.
        """
        if txn is NULL:
            txn = self.txn

        key_v.mv_data = key
        key_v.mv_size = key_size
        data_v.mv_data = data
        data_v.mv_size = data_size

        #logger.debug('Putting: {}, {} into DB {}'.format(key[: key_size],
        #    data[: data_size], dblabel))
        rc = lmdb.mdb_put(txn, self.get_dbi(dblabel), &key_v, &data_v, flags)
        _check(rc, 'Error putting data: {}, {}'.format(
                key[: key_size], data[: data_size]))


    cpdef bytes get_data(self, key, DbLabel dblabel=''):
        """
        Get a single value (non-dup) for a key (Python-facing method).
        """
        cdef lmdb.MDB_val rv
        try:
            self._get_data(key, len(key), &rv, dblabel=dblabel)

            return (<unsigned char *>rv.mv_data)[: rv.mv_size]
        except KeyNotFoundError:
            return None


    cdef void _get_data(
            self, unsigned char *key, size_t klen, lmdb.MDB_val *rv,
            DbLabel dblabel='') except *:
        """
        Get a single value (non-dup) for a key.
        """
        cdef:
            unsigned char *ret

        key_v.mv_data = key
        key_v.mv_size = len(key)

        _check(
            lmdb.mdb_get(self.txn, self.get_dbi(dblabel), &key_v, rv),
            'Error getting data for key \'{}\'.'.format(key.decode()))


    def delete(self, key, dblabel=''):
        """
        Delete one single value by key. Python-facing method.
        """
        self._delete(key, len(key), dblabel)


    cdef void _delete(
            self, unsigned char *key, size_t klen,
            DbLabel dblabel=b'') except *:
        """
        Delete one single value by key from a non-dup database.

        TODO Allow deleting duplicate keys.
        """
        key_v.mv_data = key
        key_v.mv_size = klen
        try:
            _check(lmdb.mdb_del(self.txn, self.get_dbi(dblabel), &key_v, NULL))
        except KeyNotFoundError:
            pass


    cpdef dict stats(self):
        """Gather statistics about the database."""
        return self._stats()


    cdef dict _stats(self):
        """
        Gather statistics about the database.

        Cython-only, non-transaction-aware method.
        """
        cdef:
            lmdb.MDB_stat stat
            size_t entries

        lmdb.mdb_env_stat(self.dbenv, &stat)
        env_stats = <dict>stat

        db_stats = {}
        for i, dblabel in enumerate(self.dbi_labels):
            _check(
                lmdb.mdb_stat(self.txn, self.dbis[i], &stat),
                'Error getting datbase stats: {}')
            entries = stat.ms_entries
            db_stats[dblabel] = <dict>stat

        return {
            'env_stats': env_stats,
            'env_size': os.stat(self.env_path).st_size,
            'db_stats': {
                db_label: db_stats[db_label]
                for db_label in self.dbi_labels
            },
        }


    # UNFINISHED
    #cdef int _reader_list_callback(self, const unsigned char *msg, void *ctx):
    #    """
    #    Callback for reader info function.

    #    Example from py-lmdb:
    #    static int env_readers_callback(const char *msg, void *str_)
    #    {
    #        PyObject **str = str_;
    #        PyObject *s = PyUnicode_FromString(msg);
    #        PyObject *new;
    #        if(! s) {
    #            return -1;
    #        }
    #        new = PyUnicode_Concat(*str, s);
    #        Py_CLEAR(*str);
    #        *str = new;
    #        if(! new) {
    #            return -1;
    #        }
    #        return 0;
    #    }
    #    """
    #    cdef:
    #        unicode str = ctx[0].decode('utf-8')
    #        unicode s = msg.decode('utf-8')
    #    if not len(s):
    #        return -1
    #    str += s
    #    logger.info('message: {}'.format(msg))
    #    if not len(str):
    #        return -1
    #    ctx = &str


    #cpdef str reader_list(self):
    #    """
    #    Information about the reader lock table.
    #    """
    #    cdef unsigned char *ctx
    #    lmdb.mdb_reader_list(self.dbenv, <lmdb.MDB_msg_func *>self._reader_list_callback, &ctx)
    #    logger.info('Reader info: {}'.format(ctx))

    #    return (ctx).decode('ascii')


    ### CYTHON METHODS ###

    cdef void _txn_begin(self, write=True, lmdb.MDB_txn *parent=NULL) except *:
        if not self.is_open:
            raise LmdbError('Store is not open.')

        cdef:
            unsigned int flags

        flags = 0 if write else lmdb.MDB_RDONLY

        logger.debug('Opening {} transaction in PID {}, thread {}'.format(
            'RW' if write else 'RO',
            multiprocessing.current_process().pid,
            threading.currentThread().getName()))
        #logger.debug('Readers: {}'.format(self.reader_list()))
        rc = lmdb.mdb_txn_begin(self.dbenv, parent, flags, &self.txn)
        _check(rc, 'Error opening transaction.')
        logger.debug('Opened transaction @ {:x}'.format(<unsigned long>self.txn))

        self.is_txn_open = True
        self.is_txn_rw = write
        logger.debug('txn is open: {}'.format(self.is_txn_open))


    cdef void _txn_commit(self) except *:
        txid = '{:x}'.format(<unsigned long>self.txn)
        try:
            _check(lmdb.mdb_txn_commit(self.txn))
            logger.debug('Transaction @ {} committed.'.format(txid))
            self.is_txn_open = False
            self.is_txn_rw = False
        except:
            self._txn_abort()
            raise


    cdef void _txn_abort(self) except *:
        txid = '{:x}'.format(<unsigned long>self.txn)
        lmdb.mdb_txn_abort(self.txn)
        self.is_txn_open = False
        self.is_txn_rw = False
        logger.info('Transaction @ {} aborted.'.format(txid))


    cpdef int txn_id(self):
        return self._txn_id()


    cdef size_t _txn_id(self) except -1:
        return lmdb.mdb_txn_id(self.txn)


    cdef lmdb.MDB_dbi get_dbi(
            self, DbLabel dblabel=NULL, lmdb.MDB_txn *txn=NULL):
        """
        Return a DB handle by database name.
        """
        cdef size_t dbidx

        if txn is NULL:
            txn = self.txn

        if dblabel is NULL:
            logger.debug('Getting DBI without label.')
        dbidx = (
            0 if dblabel is NULL
            else self.dbi_labels.index(dblabel)
        )
        #logger.debug(
        #        f'Got DBI {self.dbis[dbidx]} with label {dblabel} '
        #        f'and index #{dbidx}')

        return self.dbis[dbidx]


    cdef lmdb.MDB_cursor *_cur_open(
            self, DbLabel dblabel=NULL, lmdb.MDB_txn *txn=NULL) except *:
        cdef:
            lmdb.MDB_dbi dbi

        if txn is NULL:
            txn = self.txn

        dbi = self.get_dbi(dblabel, txn=txn)

        #logger.debug(f'Opening cursor for DB {dblabel} (DBI {dbi})...')
        #try:
        #    # FIXME Either reuse the cursor, if it works, or remove this code.
        #    _check(lmdb.mdb_cursor_renew(txn, self.curs[dbi]))
        #    logger.debug(f'Repurposed existing cursor for DBI {dbi}.')
        #except LmdbError as e:
        #    _check(
        #            lmdb.mdb_cursor_open(txn, dbi, self.curs + dbi),
        #            f'Error opening cursor: {dblabel}')
        #    logger.debug(f'Created brand new cursor for DBI {dbi}.')
        _check(
                lmdb.mdb_cursor_open(txn, dbi, self.curs + dbi),
                f'Error opening cursor: {dblabel}')
        #logger.debug('...opened @ {:x}.'.format(<unsigned long>self.curs[dbi]))

        return self.curs[dbi]


    cdef void _cur_close(self, lmdb.MDB_cursor *cur) except *:
        """Close a cursor."""
        #logger.info('Closing cursor @ {:x} for DBI {}...'.format(
        #    <unsigned long>cur, lmdb.mdb_cursor_dbi(cur) ))
        lmdb.mdb_cursor_close(cur)
        #logger.info('...closed.')

