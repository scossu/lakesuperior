from lakesuperior.cy_include cimport cylmdb as lmdb

cdef class BaseLmdbStore:
    cdef:
        lmdb.MDB_env *dbenv
        lmdb.MDB_txn *txn
        unsigned int readers
        lmdb.MDB_dbi **dbis

        void _init_dbis(self, create=?)
        lmdb.MDB_dbi *get_dbi(self, str dbname)
        lmdb.MDB_cursor *_cur_open(self, lmdb.MDB_txn *txn, str dbname=?)
        lmdb.MDB_txn *_txn_begin(self, write=?, lmdb.MDB_txn *parent=?)

