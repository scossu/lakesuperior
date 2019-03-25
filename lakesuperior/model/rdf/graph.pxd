from libc.stdint cimport uint32_t, uint64_t

from cymem.cymem cimport Pool

cimport lakesuperior.cy_include.collections as cc

from lakesuperior.model.base cimport Key, TripleKey
from lakesuperior.model.rdf.triple cimport BufferTriple
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.store.ldp_rs cimport lmdb_triplestore

# Callback for an iterator.
ctypedef void (*lookup_callback_fn_t)(
    Graph gr, const TripleKey* spok_p, void* ctx
)

cdef class Graph:
    cdef:
        readonly lmdb_triplestore.LmdbTriplestore store
        public Keyset keys
        public object uri

        cc.key_compare_ft term_cmp_fn
        cc.key_compare_ft trp_cmp_fn

        void _match_ptn_callback(
            self, pattern, Graph gr, lookup_callback_fn_t callback_fn,
            bint callback_cond=*, void* ctx=*
        ) except *

    cpdef Graph copy(self, str uri=*)
    cpdef Graph empty_copy(self, str uri=*)
    cpdef void set(self, tuple trp) except *


cdef:
    void add_trp_callback(Graph gr, const TripleKey* spok_p, void* ctx)
