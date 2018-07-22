# cython: language_level = 3

from lakesuperior.cy_include cimport cylmdb as lmdb

cdef:
    int rc
    size_t i

    lmdb.MDB_val key_v, data_v
    lmdb.MDB_dbi dbi

    void _check(int rc, str message) except *


cdef class BaseLmdbStore:
    cdef:
        unsigned int readers
        bytes dbpath
        lmdb.MDB_dbi *dbis
        lmdb.MDB_env *dbenv
        lmdb.MDB_txn *txn

        void _cur_close(self, lmdb.MDB_cursor *cur) except *
        void _init_dbis(self, create=*) except *
        void _txn_abort(self) except *
        void _txn_begin(self, write=*, lmdb.MDB_txn *parent=*) except *
        void _txn_commit(self) except *
        size_t _txn_id(self) except -1
        lmdb.MDB_cursor *_cur_open(self, lmdb.MDB_txn *txn, str dbname=*)
        lmdb.MDB_dbi *get_dbi(self, str dbname=*)

    cpdef bint key_exists(self, unsigned char *key, db=*) except -1
    cpdef get_data(self, unsigned char *key, db=*)
    cpdef get_dup_data(self, unsigned char *key, db=*)
    cpdef get_all_pairs(self, db=*)
    cpdef put(self, unsigned char *key, unsigned char *data, db=*, flags=*)
    cpdef stats(self)
