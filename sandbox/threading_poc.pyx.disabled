# distutils: include_dirs = ../include
# distutils: library_dirs = ../lib, ../include

import multiprocessing
import threading
import time

cimport cylmdb as lmdb

from cython.parallel cimport prange, parallel

cdef:
    lmdb.MDB_env *env
    lmdb.MDB_dbi dbi


cdef void _check(int rc) except *:
    if rc != lmdb.MDB_SUCCESS:
        out_msg = 'LMDB Error ({}): {}'.format(
            rc, lmdb.mdb_strerror(rc).decode())
        raise RuntimeError(out_msg)


cpdef void get_() except *:
    cdef:
        unsigned int flags = 0
        lmdb.MDB_txn *txn
        lmdb.MDB_val key_v, data_v
        lmdb.MDB_env *env

    _check(lmdb.mdb_env_create(&env))
    _check(lmdb.mdb_env_open(env, '/tmp/test_mp', flags, 0o644))

    print('Transaction address: {:x}'.format(<unsigned long>txn))
    _check(lmdb.mdb_txn_begin(env, NULL, lmdb.MDB_RDONLY, &txn))

    key_v.mv_data = b'a'
    key_v.mv_size = 1

    #_check(lmdb.mdb_get(txn, dbi, &key_v, &data_v))
    #print((<unsigned char *>data_v.mv_data)[:data_v.mv_size])
    time.sleep(1)
    _check(lmdb.mdb_txn_commit(txn))
    print('Txn {:x} in thread {} in process {} done.'.format(
        <unsigned long>txn,
        threading.currentThread().getName(),
        multiprocessing.current_process().name))


def run():
    cdef:
        #unsigned int flags = lmdb.MDB_NOLOCK
        #unsigned int flags = lmdb.MDB_NOTLS
        unsigned int flags = 0
        lmdb.MDB_txn *wtxn
        lmdb.MDB_val key_v, data_v

    # Set up environment.
    _check(lmdb.mdb_env_create(&env))
    #_check(lmdb.mdb_env_set_maxreaders(env, 128))
    _check(lmdb.mdb_env_open(env, '/tmp/test_mp', flags, 0o644))

    # Create DB.
    _check(lmdb.mdb_txn_begin(env, NULL, 0, &wtxn))
    _check(lmdb.mdb_dbi_open(wtxn, NULL, lmdb.MDB_CREATE, &dbi))

    # Write something.
    key_v.mv_data = b'a'
    key_v.mv_size = 1
    ts = str(time.time()).encode()
    data_v.mv_data = <unsigned char *>ts
    data_v.mv_size = len(ts)
    _check(lmdb.mdb_put(wtxn, dbi, &key_v, &data_v, 0))
    _check(lmdb.mdb_txn_commit(wtxn))
    lmdb.mdb_env_close(env)


    #print('Threaded jobs:')
    #for i in range(100):
    #    threading.Thread(target=get_).start()
    print('Multiprocess jobs:')
    for i in range(10):
        multiprocessing.Process(target=get_).start()
