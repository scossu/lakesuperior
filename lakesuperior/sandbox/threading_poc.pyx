# distutils: include_dirs = ../include
# distutils: library_dirs = ../lib, ../include

import time
cimport cylmdb as lmdb

import threading
import multiprocessing

cdef:
    long txn_addr
    lmdb.MDB_env *env


cpdef void _check(int rc, str message='') except *:
    """
    Check return code.
    """
    if rc != lmdb.MDB_SUCCESS:
        out_msg = (
                message + '\nInternal error ({}): '.format(rc)
                if len(message) else 'LMDB Error ({}): '.format(rc))
        out_msg += lmdb.mdb_strerror(rc).decode()
        raise RuntimeError(out_msg)


cpdef void get_():
    cdef:
        lmdb.MDB_txn *txn
        lmdb.MDB_txn *wtxn
        lmdb.MDB_dbi dbi, wdbi
        lmdb.MDB_val key_v, data_v

    _check(lmdb.mdb_txn_begin(env, NULL, 0, &txn))
    _check(lmdb.mdb_dbi_open(txn, NULL, 0, &dbi))
    _check(lmdb.mdb_txn_begin(env, txn, 0, &wtxn))
    _check(lmdb.mdb_dbi_open(wtxn, NULL, 0, &wdbi))

    ts = str(time.time()).encode()
    key_v.mv_data = b'a'
    key_v.mv_size = 1
    data_v.mv_data = <unsigned char *>ts
    data_v.mv_size = len(ts)

    _check(lmdb.mdb_put(wtxn, wdbi, &key_v, &data_v, 0))
    _check(lmdb.mdb_txn_commit(wtxn))

    key_v.mv_data = b'a'
    key_v.mv_size = 1
    _check(lmdb.mdb_get(txn, dbi, &key_v, &data_v))
    print((<unsigned char *>data_v.mv_data)[:data_v.mv_size])
    time.sleep(2)
    print('Thread {} in process {} done.'.format(threading.currentThread().getName(), multiprocessing.current_process().name))
    lmdb.mdb_txn_commit(txn)

def run():
    flags = lmdb.MDB_NOTLS
    #flags = 0
    _check(lmdb.mdb_env_create(&env))
    _check(lmdb.mdb_env_open(env, '/tmp/test_nest', flags, 0o644))

    print('Threaded jobs:')
    for i in range(10):
        threading.Thread(target=get_).start()
    print('Multiprocess jobs:')
    for i in range(10):
        multiprocessing.Process(target=get_).start()
    lmdb.mdb_env_close(env)
