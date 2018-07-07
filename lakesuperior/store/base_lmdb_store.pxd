# cython: language_level = 3

from lakesuperior.cy_include cimport cylmdb as lmdb

cdef:
    int rc
    size_t i

    _check(int rc, str message)


cdef class BaseLmdbStore:
    cdef:
        unsigned int readers
        bytes dbpath
        lmdb.MDB_dbi *dbis
        lmdb.MDB_env *dbenv
        lmdb.MDB_txn *txn

        void _cur_close(self, lmdb.MDB_cursor *cur)
        void _init_dbis(self, create=*)
        void _txn_commit(self)
        void _txn_abort(self)
        lmdb.MDB_cursor *_cur_open(self, lmdb.MDB_txn *txn, str dbname=*)
        lmdb.MDB_dbi *get_dbi(self, str dbname=*)
        void _txn_begin(self, write=*, lmdb.MDB_txn *parent=*)

    cpdef get_value(self, key, db=*)
    cpdef get_dup_values(self, key, db=*)
    cpdef get_all_pairs(self, db=*)
