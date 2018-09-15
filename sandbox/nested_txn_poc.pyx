import multiprocessing
import threading
import time

from hashlib import sha1

from lakesuperior.cy_include cimport cylmdb as lmdb

from os import makedirs
from shutil import rmtree

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cython.parallel cimport prange, parallel
from libc.string cimport memcmp, memcpy, strchr

DEF HSIZE = 20

cdef:
    lmdb.MDB_env *env
    lmdb.MDB_dbi dbi


cdef void _check(int rc) except *:
    if rc != lmdb.MDB_SUCCESS:
        out_msg = 'LMDB Error ({}): {}'.format(
            rc, lmdb.mdb_strerror(rc).decode())
        raise RuntimeError(out_msg)


ctypedef struct vals:
    unsigned char *data
    size_t size

cpdef paging(size_t ct):
    cdef:
        unsigned char *k
        unsigned char *v
        unsigned char *datastream
        unsigned int dbi_flags = lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED | lmdb.MDB_CREATE
        size_t i = 0, offset
        size_t *offsets
        lmdb.MDB_txn *txn
        lmdb.MDB_cursor *cur
        lmdb.MDB_dbi maxdbs = 32
        lmdb.MDB_val key_v, data_v, data1_v, data2_v
        lmdb.MDB_val data_mv[2]

    env_path = '/tmp/test_paging'
    flags = 0
    #flags = lmdb.MDB_NOTLS
    #flags = lmdb.MDB_NOLOCK

    # Delete previous files.
    rmtree(env_path, ignore_errors=True)
    makedirs(env_path)

    # Set up environment.
    _check(lmdb.mdb_env_create(&env))
    _check(lmdb.mdb_env_set_maxdbs(env, maxdbs))
    _check(lmdb.mdb_env_open(env, env_path.encode(), 0, 0o644))

    # Create DB.
    _check(lmdb.mdb_txn_begin(env, NULL, 0, &txn))
    _check(lmdb.mdb_dbi_open(txn, b'db1', dbi_flags, &dbi))

    # Write something.
    key_v.mv_data = b'a'
    key_v.mv_size = 1
    data1_v.mv_size = HSIZE
    data2_v.mv_size = ct

    #offsets = <size_t *>PyMem_Malloc(ct * sizeof(size_t))
    datastream = <unsigned char *>PyMem_Malloc(ct * HSIZE)
    for i in range(ct):
        _v = sha1(chr(i).encode()).digest()
        v = _v
        offset = i * HSIZE
        #print('Inserting {} in datastream[{}].'.format(v, offset))
        memcpy(datastream + offset, v, HSIZE)

    data1_v.mv_data = datastream
    data2_v.mv_data = NULL
    #print('Datastream: {} ({})'.format(datastream[: (ct * HSIZE)], ct * HSIZE))
    data_mv = [data1_v, data2_v]

    _check(lmdb.mdb_cursor_open(txn, dbi, &cur))
    _check(lmdb.mdb_cursor_put(cur, &key_v, data_mv, lmdb.MDB_MULTIPLE))

    _check(lmdb.mdb_txn_commit(txn))

    _check(lmdb.mdb_txn_begin(env, NULL, lmdb.MDB_RDONLY, &txn))
    _check(lmdb.mdb_cursor_open(txn, dbi, &cur))
    i = 0
    _check(lmdb.mdb_cursor_get(cur, &key_v, NULL, lmdb.MDB_SET))
    while lmdb.mdb_cursor_get(cur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE) == lmdb.MDB_SUCCESS:
        print('Retrieved data page #{} for key {}: {} ({} bytes)'.format(
            i, (<unsigned char *>key_v.mv_data)[:key_v.mv_size],
            (<unsigned char *>data_v.mv_data)[:data_v.mv_size],
            data_v.mv_size))
        i += 1
    lmdb.mdb_cursor_close(cur)
    lmdb.mdb_txn_abort(txn)

    lmdb.mdb_env_close(env)
    #PyMem_Free(offsets)
    PyMem_Free(datastream)


#def nested():
#    cdef:
#        unsigned int flags
#        lmdb.MDB_txn *txn
#        lmdb.MDB_txn *wtxn
#        lmdb.MDB_val key_v, data_v
#
#    env_path = '/tmp/test_nested_txn'
#    #flags = 0
#    #flags = lmdb.MDB_NOTLS
#    flags = lmdb.MDB_NOLOCK
#
#    # Delete previous files.
#    rmtree(env_path, ignore_errors=True)
#    makedirs(env_path)
#
#    # Set up environment.
#    _check(lmdb.mdb_env_create(&env))
#    _check(lmdb.mdb_env_open(env, env_path.encode(), flags, 0o644))
#
#    # Create DB.
#    _check(lmdb.mdb_txn_begin(env, NULL, lmdb.MDB_RDONLY, &txn))
#    _check(lmdb.mdb_txn_begin(env, txn, 0, &wtxn))
#    _check(lmdb.mdb_dbi_open(wtxn, NULL, lmdb.MDB_CREATE, &dbi))
#
#    # Write something.
#    key_v.mv_data = b'a'
#    key_v.mv_size = 1
#    ts = str(time.time()).encode()
#    data_v.mv_data = <unsigned char *>ts
#    data_v.mv_size = len(ts)
#    _check(lmdb.mdb_put(wtxn, dbi, &key_v, &data_v, 0))
#
#    _check(lmdb.mdb_txn_commit(wtxn))
#    _check(lmdb.mdb_txn_commit(txn))
#    #lmdb.mdb_txn_reset(txn) # This won't work.
#
#    _check(lmdb.mdb_txn_begin(env, NULL, 0, &txn))
#    #_check(lmdb.mdb_txn_renew(txn)) # This won't work.
#    data_v.mv_data = NULL
#    _check(lmdb.mdb_get(txn, dbi, &key_v, &data_v))
#    print('Retrieved data: {}'.format(
#        (<unsigned char *>data_v.mv_data)[:data_v.mv_size]))
#    _check(lmdb.mdb_txn_commit(txn))
#    lmdb.mdb_env_close(env)
