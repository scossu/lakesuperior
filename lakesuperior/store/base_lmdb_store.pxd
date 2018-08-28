# cython: language_level = 3

from lakesuperior.cy_include cimport cylmdb as lmdb

cdef:
    int rc
    size_t i
    bint is_txn_open

    lmdb.MDB_val key_v, data_v
    lmdb.MDB_dbi dbi

    void _check(int rc, str message=*) except *


cdef class BaseLmdbStore:
    cdef:
        public bint _open
        unsigned int _readers
        bytes dbpath
        lmdb.MDB_dbi *dbis
        lmdb.MDB_env *dbenv
        lmdb.MDB_txn *txn

        void _clear_stale_readers(self) except *
        void _cur_close(self, lmdb.MDB_cursor *cur) except *
        void _init_dbis(self, create=*) except *
        void _txn_begin(self, write=*, lmdb.MDB_txn *parent=*) except *
        void _txn_commit(self) except *
        void _txn_abort(self) except *
        inline bint _key_exists(
            self, const unsigned char *key, unsigned char klen,
            unsigned char *dblabel=*) except -1

        size_t _txn_id(self) except -1
        lmdb.MDB_cursor *_cur_open(
                self, unsigned char *dblabel=*, lmdb.MDB_txn *txn=*) except *

        lmdb.MDB_dbi get_dbi(
                self, unsigned char *dblabel=*, lmdb.MDB_txn *txn=*)

        void _put(
                self, unsigned char *key, size_t key_size, unsigned char *data,
                size_t data_size, unsigned char *dblabel=*,
                lmdb.MDB_txn *txn=*, unsigned int flags=*) except *

        void _get_data(
                self, unsigned char *key, size_t klen, lmdb.MDB_val *rv,
                unsigned char *dblabel=*) except *

        void _delete(
                self, unsigned char *key, size_t klen,
                unsigned char *dblabel=*) except *

        dict _stats(self)
        #int _reader_list_callback(self, const unsigned char *msg, void *str_)

    cpdef void close_env(self, bint commit_pending_transaction=*) except *
    cpdef void destroy(self, _path=*) except *
    #cpdef get_dup_data(self, unsigned char *key, db=*)
    #cpdef get_all_pairs(self, db=*)
    cpdef bytes get_data(self, key, dblabel=*)
    cpdef dict stats(self, new_txn=*)
    cpdef int txn_id(self)
    #cpdef str reader_list(self)
