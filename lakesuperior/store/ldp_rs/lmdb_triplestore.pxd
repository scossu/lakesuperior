cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.cy_include.cylmdb as lmdb
cimport lakesuperior.cy_include.cytpl as tpl

from lakesuperior.model.base cimport (
    Key, DoubleKey, TripleKey, Buffer
)
from lakesuperior.model.graph.graph cimport Graph
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.store.base_lmdb_store cimport BaseLmdbStore

cdef:
    enum:
        IDX_OP_ADD = 1
        IDX_OP_REMOVE = -1

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

    cdef:
        void _index_triple(self, int op, TripleKey spok) except *
        Graph triple_keys(self, tuple triple_pattern, str context=*, str uri=*)
        void _all_term_keys(self, term_type, cc.HashSet** tkeys) except *
        void lookup_term(self, const Key* tk, Buffer* data) except *
        Graph _lookup(self, tuple triple_pattern)
        Graph _lookup_1bound(self, unsigned char idx, Key luk)
        Graph _lookup_2bound(
            self, unsigned char idx1, unsigned char idx2, DoubleKey tks
        )
        object from_key(self, const Key tk)
        Key to_key(self, term) except -1
        void all_contexts(self, Key** ctx, size_t* sz, triple=*) except *
        Key _append(
                self, Buffer *value,
                unsigned char *dblabel=*, lmdb.MDB_txn *txn=*,
                unsigned int flags=*)
