from libc.stdint cimport uint32_t, uint64_t

from cymem.cymem cimport Pool

cimport lakesuperior.cy_include.collections as cc

from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph.triple cimport BufferTriple

# Lookup function that returns whether a triple contains a match pattern.
# Return True if the triple exists, False otherwise.
ctypedef bint (*lookup_fn_t)(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2)

# Callback for an iterator.
ctypedef void (*lookup_callback_fn_t)(
    Graph gr, const BufferTriple* trp, void* ctx
)

ctypedef Buffer SPOBuffer[3]
ctypedef Buffer *BufferPtr

cdef class Graph:
    cdef:
        cc.HashSet *_terms # Set of unique serialized terms.
        cc.HashSet *_triples # Set of unique triples.
        # Temp data pool. It gets managed with the object lifecycle via cymem.
        Pool pool

        cc.key_compare_ft term_cmp_fn
        cc.key_compare_ft trp_cmp_fn

        BufferTriple* store_triple(self, const BufferTriple* strp)
        void add_triple(
            self, const BufferTriple *trp, bint copy=*
        ) except *
        int remove_triple(self, const BufferTriple* trp_buf) except -1
        bint trp_contains(self, const BufferTriple* btrp)

        # Basic graph operations.
        void ip_union(self, Graph other) except *
        void ip_subtraction(self, Graph other) except *
        void ip_intersection(self, Graph other) except *
        void ip_xor(self, Graph other) except *
        Graph empty_copy(self)
        void _match_ptn_callback(
            self, pattern, Graph gr,
            lookup_callback_fn_t callback_fn, void* ctx=*
        ) except *

    cpdef union_(self, Graph other)
    cpdef subtraction(self, Graph other)
    cpdef intersection(self, Graph other)
    cpdef xor(self, Graph other)
    cpdef void set(self, tuple trp) except *


cdef class Imr(Graph):
    cdef:
        readonly str id
        Imr empty_copy(self)

    cpdef as_rdflib(self)
