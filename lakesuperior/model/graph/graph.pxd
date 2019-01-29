from libc.stdint cimport uint32_t, uint64_t

from cymem.cymem cimport Pool

from lakesuperior.cy_include.collections cimport (
    HashSet, HashSetConf,
    #_hash_ft, _key_compare_ft, _mem_alloc_ft, _mem_calloc_ft, _mem_free_ft,
)
from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph.triple cimport BufferTriple
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.store.ldp_rs.lmdb_triplestore cimport LmdbTriplestore
from lakesuperior.store.ldp_rs.lmdb_triplestore cimport TripleKey

# Lookup function that returns whether a triple contains a match pattern.
ctypedef bint (*lookup_fn_t)(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2)

ctypedef Buffer SPOBuffer[3]
ctypedef Buffer *BufferPtr

cdef:
    int term_cmp_fn(const void* key1, const void* key2)
    int triple_cmp_fn(const void* key1, const void* key2)
    size_t trp_hash_fn(const void* key, int l, uint32_t seed)
    size_t hash_ptr_passthrough(const void* key, int l, uint32_t seed)

cdef class SimpleGraph:
    cdef:
        HashSet *_terms # Set of unique serialized terms.
        HashSet *_triples # Set of unique triples.
        readonly LmdbTriplestore store
        # Temp data pool. It gets managed with the object lifecycle via cymem.
        Pool _pool

        void _data_from_lookup(self, tuple trp_ptn, ctx=*) except *
        void _data_from_keyset(self, Keyset data) except *
        inline void _add_from_spok(self, const TripleKey spok) except *
        inline void _add_triple(
            self, Buffer *ss, Buffer *sp, Buffer *so
        ) except *
        int _add_or_get_term(self, Buffer **data) except -1
        set _data_as_set(self)

    cpdef void set(self, tuple trp) except *
    cpdef void remove_triples(self, pattern) except *
    cpdef object as_rdflib(self)
    cpdef set terms(self, str type)

cdef class Imr(SimpleGraph):
    cdef:
        readonly str uri

    cpdef as_rdflib(self)
