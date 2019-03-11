cimport lakesuperior.cy_include.cylmdb as lmdb
cimport lakesuperior.cy_include.cytpl as tpl

from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph.graph cimport SimpleGraph
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.store.base_lmdb_store cimport BaseLmdbStore

# NOTE This may change in the future, e.g. if a different key size is to
# be forced.
ctypedef size_t KeyIdx

ctypedef KeyIdx Key[1]
ctypedef KeyIdx DoubleKey[2]
ctypedef KeyIdx TripleKey[3]
ctypedef KeyIdx QuadKey[4]

cdef enum:
    KLEN = sizeof(Key)
    DBL_KLEN = sizeof(DoubleKey)
    TRP_KLEN = sizeof(TripleKey)
    QUAD_KLEN = sizeof(QuadKey)

    IDX_OP_ADD = 1
    IDX_OP_REMOVE = -1

    INT_KEY_MASK = (
        lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED | lmdb.MDB_INTEGERKEY
        | lmdb.MDB_REVERSEKEY # TODO Check endianness.
    )
    INT_DUP_MASK = (
        lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED | lmdb.MDB_INTEGERDUP
        | lmdb.MDB_REVERSEDUP # TODO Check endianness.
    )

cdef:
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
    cpdef SimpleGraph graph_lookup(
        self, triple_pattern, context=*, uri=*, copy=*
    )

    cdef:
        void _add_graph(self, Buffer *pk_gr) except *
        void _index_triple(self, str op, TripleKey spok) except *
        Keyset triple_keys(self, tuple triple_pattern, context=*)
        Keyset _all_term_keys(self, term_type)
        inline void lookup_term(self, const Key key, Buffer* data) except *
        Keyset _lookup(self, tuple triple_pattern)
        Keyset _lookup_1bound(self, unsigned char idx, term)
        Keyset _lookup_2bound(
                self, unsigned char idx1, term1, unsigned char idx2, term2)
        object from_key(self, const Key key)
        tuple from_trp_key(self, TripleKey key)
        Key _to_key(self, term)
        void _to_triple_key(
                self, tuple terms, TripleKey *tkey) except *
        KeyIdx _append(
                self, Buffer *value,
                unsigned char *dblabel=*, lmdb.MDB_txn *txn=*,
                unsigned int flags=*)
