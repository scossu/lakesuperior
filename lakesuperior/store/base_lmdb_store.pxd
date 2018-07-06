# cython: language_level = 3

from lakesuperior.cy_include cimport cylmdb as lmdb

cdef:
    int rc
    size_t i


cdef class BaseLmdbStore:
    cdef:
        lmdb.MDB_env *dbenv
        lmdb.MDB_txn *txn
        unsigned int readers
        lmdb.MDB_dbi *dbis
        bytes dbpath

        void _init_dbis(self, create=*)
        lmdb.MDB_dbi *get_dbi(self, char *dbname)
        lmdb.MDB_cursor *_cur_open(self, lmdb.MDB_txn *txn, char *dbname=*)
        void _cur_close(self, lmdb.MDB_cursor *cur)
        lmdb.MDB_txn *_txn_begin(self, write=*, lmdb.MDB_txn *parent=*)

