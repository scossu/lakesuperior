from libc.stdint cimport uint32_t, uint64_t

from cymem.cymem cimport Pool

from lakesuperior.cy_include cimport collections as cc
from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph.triple cimport BufferTriple
from lakesuperior.model.structures.keyset cimport Keyset

# Lookup function that returns whether a triple contains a match pattern.
ctypedef bint (*lookup_fn_t)(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2)

ctypedef Buffer SPOBuffer[3]
ctypedef Buffer *BufferPtr

cdef:
    int term_cmp_fn(const void* key1, const void* key2)
    int trp_cmp_fn(const void* key1, const void* key2)
    bint graph_eq_fn(SimpleGraph g1, SimpleGraph g2)
    size_t trp_hash_fn(const void* key, int l, uint32_t seed)
    size_t hash_ptr_passthrough(const void* key, int l, uint32_t seed)

cdef class SimpleGraph:
    cdef:
        cc.HashSet *_terms # Set of unique serialized terms.
        cc.HashSet *_triples # Set of unique triples.
        # Temp data pool. It gets managed with the object lifecycle via cymem.
        Pool pool

        cc.key_compare_ft term_cmp_fn
        cc.key_compare_ft trp_cmp_fn

        inline void add_triple(self, BufferTriple *trp) except *
        int remove_triple(self, BufferTriple* trp_buf) except -1
        bint trp_contains(self, BufferTriple* btrp)

        # Basic graph operations.
        void ip_union(self, SimpleGraph other) except *
        void ip_subtraction(self, SimpleGraph other) except *
        void ip_intersection(self, SimpleGraph other) except *
        void ip_xor(self, SimpleGraph other) except *
        SimpleGraph empty_copy(self)

    cpdef union_(self, SimpleGraph other)
    cpdef subtraction(self, SimpleGraph other)
    cpdef intersection(self, SimpleGraph other)
    cpdef xor(self, SimpleGraph other)
    cpdef void set(self, tuple trp) except *
    cpdef void remove_triples(self, pattern) except *


cdef class Imr(SimpleGraph):
    cdef:
        readonly str uri
        Imr empty_copy(self)

    cpdef as_rdflib(self)
