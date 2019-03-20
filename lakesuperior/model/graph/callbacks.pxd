from libc.stdint cimport uint32_t, uint64_t

from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph cimport graph
from lakesuperior.model.graph.triple cimport BufferTriple

cdef extern from 'spookyhash_api.h':
    uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed)

cdef:
    bint graph_eq_fn(graph.SimpleGraph g1, graph.SimpleGraph g2)
    int term_cmp_fn(const void* key1, const void* key2)
    int trp_cmp_fn(const void* key1, const void* key2)
    size_t term_hash_fn(const void* key, int l, uint32_t seed)
    size_t trp_hash_fn(const void* key, int l, uint32_t seed)

    bint lookup_none_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    bint lookup_s_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    bint lookup_p_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    bint lookup_o_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    bint lookup_sp_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    bint lookup_so_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    bint lookup_po_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2
    )
    void add_trp_callback(
        graph.SimpleGraph gr, const BufferTriple* trp, void* ctx
    )
    void del_trp_callback(
        graph.SimpleGraph gr, const BufferTriple* trp, void* ctx
    )

