import hashlib
import logging

from lakesuperior.store.base_lmdb_store import BaseLmdbStore, LmdbError

from libc cimport errno

from lakesuperior.cy_include cimport cylmdb as lmdb

cdef class LmdbTriplestore(BaseLmdbStore):

    db_config = {
        # Main data
        # Term key to serialized term content
        't:st': 0,
        # Joined triple keys to context key
        'spo:c': 0,
        # This has empty values and is used to keep track of empty contexts.
        'c:': 0,
        # Prefix to namespace
        'pfx:ns': 0,

        # Indices
        # Namespace to prefix
        'ns:pfx': 0,
        # Term hash to triple key
        'th:t': 0,
        # Lookups
        's:po': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'p:so': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'o:sp': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'c:spo': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
    }

    flags = lmdb.MDB_NOSUBDIR | lmdb.MDB_NORDAHEAD

    options = {
        'map_size': 1024 ** 4 # 1Tb.
    }
