cimport lakesuperior.cy_include.cylmdb as lmdb
cimport lakesuperior.cy_include.cytpl as tpl

from lakesuperior.model.base cimport Buffer
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.store.base_lmdb_store cimport BaseLmdbStore

#Fixed length for term keys.
#
#4 or 5 is a safe range. 4 allows for ~4 billion (256 ** 4) unique terms
#in the store. 5 allows ~1 trillion terms. While these numbers may seem
#huge (the total number of Internet pages indexed by Google as of 2018 is 45
#billions), it must be reminded that the keys cannot be reused, so a
#repository that deletes a lot of triples may burn through a lot of terms.
#
#If a repository runs ot of keys it can no longer store new terms and must
#be migrated to a new database, which will regenerate and compact the keys.
#
#For smaller repositories it should be safe to set this value to 4, which
#could improve performance since keys make up the vast majority of record
#exchange between the store and the application. However it is sensible not
#to expose this value as a configuration option.
#
#TODO: Explore the option to use size_t (8 bits, or in some architectures,
#4 bits). If the overhead of handling 8
#vs. 5 bytes is not huge (and maybe counterbalanced by x86_64 arch optimizations
#for 8-byte words) it may be worth using those instead of char[5] to simplify
#the code significantly.
DEF _KLEN = 5
DEF _DBL_KLEN = _KLEN * 2
DEF _TRP_KLEN = _KLEN * 3
DEF _QUAD_KLEN = _KLEN * 4
# Lexical sequence start. ``\\x01`` is fine since no special characters are
# used, but it's good to leave a spare for potential future use.
DEF _KEY_START = b'\x01'

cdef enum:
    KLEN = _KLEN
    DBL_KLEN = _DBL_KLEN
    TRP_KLEN = _TRP_KLEN
    QUAD_KLEN = _QUAD_KLEN

ctypedef unsigned char Key[KLEN]
ctypedef unsigned char DoubleKey[DBL_KLEN]
ctypedef unsigned char TripleKey[TRP_KLEN]
ctypedef unsigned char QuadKey[QUAD_KLEN]

cdef:
    unsigned char KEY_START = _KEY_START
    unsigned char FIRST_KEY[KLEN]
    unsigned char lookup_rank[3]
    unsigned char lookup_ordering[3][3]
    unsigned char lookup_ordering_2bound[3][3]


cdef class LmdbTriplestore(BaseLmdbStore):
    cpdef dict stats(self)
    cpdef size_t _len(self, context=*) except -1
    cpdef add(self, triple, context=*, quoted=*)
    cpdef add_graph(self, graph)
    cpdef void _remove(self, tuple triple_pattern, context=*) except *
    cpdef void _remove_graph(self, object gr_uri) except *
    cpdef tuple all_namespaces(self)
    cpdef tuple all_contexts(self, triple=*)

    cdef:
        void _add_graph(self, Buffer *pk_gr) except *
        void _index_triple(self, str op, TripleKey spok) except *
        Keyset triple_keys(self, tuple triple_pattern, context=*)
        Keyset _all_term_keys(self, term_type)
        inline int lookup_term(self, const Key key, Buffer *data) except -1
        Keyset _lookup(self, tuple triple_pattern)
        Keyset _lookup_1bound(self, unsigned char idx, term)
        Keyset _lookup_2bound(
                self, unsigned char idx1, term1, unsigned char idx2, term2)
        object from_key(self, const Key key)
        tuple from_trp_key(self, TripleKey key)
        inline void _to_key(self, term, Key *key) except *
        inline void _to_triple_key(self, tuple terms, TripleKey *tkey) except *
        void _append(
                self, Buffer *value, Key *nkey,
                unsigned char *dblabel=*, lmdb.MDB_txn *txn=*,
                unsigned int flags=*) except *
        void _next_key(self, const Key key, Key *nkey) except *
