# distutils: include_dirs = ../lib
# distutils: library_dirs = ../lib
# distutils: libraries = lmdb
#from posix.types cimport (blkcnt_t, blksize_t, dev_t, gid_t, ino_t, mode_t,
#                          nlink_t, off_t, time_t, uid_t)
from posix.types cimport mode_t

cdef extern from '<sys/types.h>':
    pass

cdef extern from 'lmdb.h':
    ctypedef mode_t mdb_mode_t
    ctypedef int mdb_filehandle_t
    ctypedef unsigned int MDB_dbi

    ctypedef enum MDB_cursor_op:
        MDB_FIRST,
        MDB_FIRST_DUP,
        MDB_GET_BOTH,
        MDB_GET_BOTH_RANGE,
        MDB_GET_CURRENT,
        MDB_GET_MULTIPLE,
        MDB_LAST,
        MDB_LAST_DUP,
        MDB_NEXT,
        MDB_NEXT_DUP,
        MDB_NEXT_MULTIPLE,
        MDB_NEXT_NODUP,
        MDB_PREV,
        MDB_PREV_DUP,
        MDB_PREV_NODUP,
        MDB_SET,
        MDB_SET_KEY,
        MDB_SET_RANGE,
        MDB_PREV_MULTIPLE

    ctypedef struct MDB_env:
        pass
    ctypedef struct MDB_txn:
        pass
    ctypedef struct MDB_cursor:
        pass

    ctypedef int MDB_msg_func(const char *msg, void *ctx)
    ctypedef int MDB_cmp_func(const MDB_val *a, const MDB_val *b)
    ctypedef void MDB_rel_func(
            MDB_val *item, void *oldptr, void *newptr, void *relctx)
    ctypedef void MDB_assert_func(MDB_env *env, const char *msg)

    int MDB_VERSION_MAJOR
    int MDB_VERSION_MINOR
    int MDB_VERSION_PATCH
    int MDB_VERINT(int a, int b, int c)
    int MDB_VERSION_FULL
    char MDB_VERSION_DATE
    char MDB_VERSTR(int a, int b, int c, int d)
    char MDB_VERFOO(int a, int b, int c, int d)
    char MDB_VERSION_STRING

    int MDB_FIXEDMAP
    int MDB_NOSUBDIR
    int MDB_NOSYNC
    int MDB_RDONLY
    int MDB_NOMETASYNC
    int MDB_WRITEMAP
    int MDB_MAPASYNC
    int MDB_NOTLS
    int MDB_NOLOCK
    int MDB_NORDAHEAD
    int MDB_NOMEMINIT

    int MDB_REVERSEKEY
    int MDB_DUPSORT
    int MDB_INTEGERKEY
    int MDB_DUPFIXED
    int MDB_INTEGERDUP
    int MDB_REVERSEDUP
    int MDB_CREATE

    int MDB_NOOVERWRITE
    int MDB_NODUPDATA
    int MDB_CURRENT
    int MDB_RESERVE
    int MDB_APPEND
    int MDB_APPENDDUP
    int MDB_MULTIPLE
    int MDB_CP_COMPACT

    int MDB_SUCCESS
    int MDB_KEYEXIST
    int MDB_NOTFOUND
    int MDB_PAGE_NOTFOUND
    int MDB_CORRUPTED
    int MDB_PANIC
    int MDB_VERSION_MISMATCH
    int MDB_INVALID
    int MDB_MAP_FULL
    int MDB_DBS_FULL
    int MDB_READERS_FULL
    int MDB_TLS_FULL
    int MDB_TXN_FULL
    int MDB_CURSOR_FULL
    int MDB_PAGE_FULL
    int MDB_MAP_RESIZED
    int MDB_INCOMPATIBLE
    int MDB_BAD_RSLOT
    int MDB_BAD_TXN
    int MDB_BAD_VALSIZE
    int MDB_BAD_DBI
    int MDB_LAST_ERRCODE

    struct MDB_val:
        size_t mv_size
        void *mv_data

    struct MDB_stat:
        unsigned int ms_psize
        unsigned int ms_depth
        size_t ms_branch_pages
        size_t ms_leaf_pages
        size_t ms_overflow_pages
        size_t ms_entries

    struct MDB_envinfo:
        void *me_mapaddr
        size_t me_mapsize
        size_t me_last_pgno
        size_t me_last_txnid
        unsigned int me_maxreaders
        unsigned int me_numreaders

    char *mdb_version(int *major, int *minor, int *patch)
    char *mdb_strerror(int err)
    int  mdb_env_create(MDB_env **env)
    int  mdb_env_open(
            MDB_env *env, const char *path, unsigned int flags,
            mdb_mode_t mode)
    int  mdb_env_copy(MDB_env *env, const char *path)
    int  mdb_env_copyfd(MDB_env *env, mdb_filehandle_t fd)
    int  mdb_env_copy2(MDB_env *env, const char *path, unsigned int flags)
    int  mdb_env_copyfd2(MDB_env *env, mdb_filehandle_t fd, unsigned int flags)
    int  mdb_env_stat(MDB_env *env, MDB_stat *stat)
    int  mdb_env_info(MDB_env *env, MDB_envinfo *stat)
    int  mdb_env_sync(MDB_env *env, int force)
    void mdb_env_close(MDB_env *env)
    int  mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff)
    int  mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff)
    int  mdb_env_get_path(MDB_env *env, const char **path)
    int  mdb_env_get_fd(MDB_env *env, mdb_filehandle_t *fd)
    int  mdb_env_set_mapsize(MDB_env *env, size_t size)
    int  mdb_env_set_maxreaders(MDB_env *env, unsigned int readers)
    int  mdb_env_get_maxreaders(MDB_env *env, unsigned int *readers)
    int  mdb_env_set_maxdbs(MDB_env *env, MDB_dbi dbs)
    int  mdb_env_get_maxkeysize(MDB_env *env)
    int  mdb_env_set_userctx(MDB_env *env, void *ctx)
    void *mdb_env_get_userctx(MDB_env *env)

    int  mdb_env_set_assert(MDB_env *env, MDB_assert_func *func)
    int  mdb_txn_begin(
            MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn)
    MDB_env *mdb_txn_env(MDB_txn *txn)
    size_t mdb_txn_id(MDB_txn *txn)
    int  mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)
    void mdb_txn_reset(MDB_txn *txn)
    int  mdb_txn_renew(MDB_txn *txn)

    int  mdb_open(
            MDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi)
    void mdb_close(MDB_env *env, MDB_dbi dbi)
    int  mdb_dbi_open(
            MDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi)
    int  mdb_stat(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat)
    int mdb_dbi_flags(MDB_txn *txn, MDB_dbi dbi, unsigned int *flags)
    void mdb_dbi_close(MDB_env *env, MDB_dbi dbi)
    int  mdb_drop(MDB_txn *txn, MDB_dbi dbi, int del_)
    int  mdb_set_compare(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp)
    int  mdb_set_dupsort(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp)
    int  mdb_set_relfunc(MDB_txn *txn, MDB_dbi dbi, MDB_rel_func *rel)
    int  mdb_set_relctx(MDB_txn *txn, MDB_dbi dbi, void *ctx)
    int  mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
    int  mdb_put(
            MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
            unsigned int flags)
    int  mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
    int  mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor)
    void mdb_cursor_close(MDB_cursor *cursor)
    int  mdb_cursor_renew(MDB_txn *txn, MDB_cursor *cursor)
    MDB_txn *mdb_cursor_txn(MDB_cursor *cursor)
    MDB_dbi mdb_cursor_dbi(MDB_cursor *cursor)
    int  mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
        MDB_cursor_op op)
    int  mdb_cursor_put(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
        unsigned int flags)
    int  mdb_cursor_del(MDB_cursor *cursor, unsigned int flags)
    int  mdb_cursor_count(MDB_cursor *cursor, size_t *countp)
    int  mdb_cmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b)
    int  mdb_dcmp(
            MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b)

    int mdb_reader_list(MDB_env *env, MDB_msg_func *func, void *ctx)
    int mdb_reader_check(MDB_env *env, int *dead)
