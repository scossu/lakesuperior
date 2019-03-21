import logging

from libc.stdint cimport uint32_t, uint64_t
from libc.string cimport memcmp

cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.cy_include.spookyhash as sph

from lakesuperior.model.base cimport Buffer, buffer_dump
from lakesuperior.model.graph cimport graph
from lakesuperior.model.graph.triple cimport BufferTriple

logger = logging.getLogger(__name__)


cdef inline int term_cmp_fn(const void* key1, const void* key2):
    """
    Compare function for two Buffer objects.

    :rtype: int
    :return: 0 if the byte streams are the same, another integer otherwise.
    """
    b1 = <Buffer *>key1
    b2 = <Buffer *>key2

    if b1.sz != b2.sz:
        #logger.info(f'Sizes differ: {b1.sz} != {b2.sz}. Return 1.')
        return 1

    return memcmp(b1.addr, b2.addr, b1.sz)


cdef inline int trp_cmp_fn(const void* key1, const void* key2):
    """
    Compare function for two triples in a set.

    s, p, o byte data are compared literally.

    :rtype: int
    :return: 0 if all three terms point to byte-wise identical data in both
        triples.
    """
    t1 = <BufferTriple *>key1
    t2 = <BufferTriple *>key2

    # Compare in order of probability (largest sets first).
    return (
        term_cmp_fn(t1.o, t2.o) or
        term_cmp_fn(t1.s, t2.s) or
        term_cmp_fn(t1.p, t2.p)
    )


#cdef int trp_cmp_fn(const void* key1, const void* key2):
#    """
#    Compare function for two triples in a set.
#
#    Here, pointers to terms are compared for s, p, o. The pointers should be
#    guaranteed to point to unique values (i.e. no two pointers have the same
#    term value within a graph).
#
#    :rtype: int
#    :return: 0 if the addresses of all terms are the same, 1 otherwise.
#    """
#    t1 = <BufferTriple *>key1
#    t2 = <BufferTriple *>key2
#
#    cdef int is_not_equal = (
#        t1.s.addr != t2.s.addr or
#        t1.p.addr != t2.p.addr or
#        t1.o.addr != t2.o.addr
#    )
#
#    logger.info(f'Triples match: {not(is_not_equal)}')
#    return is_not_equal


cdef bint graph_eq_fn(graph.SimpleGraph g1, graph.SimpleGraph g2):
    """
    Compare 2 graphs for equality.

    Note that this returns the opposite value than the triple and term
    compare functions: 1 (True) if equal, 0 (False) if not.
    """
    cdef:
        void* el
        cc.HashSetIter it

    cc.hashset_iter_init(&it, g1._triples)
    while cc.hashset_iter_next(&it, &el) != cc.CC_ITER_END:
        if cc.hashset_contains(g2._triples, el):
            return False

    return True


cdef size_t term_hash_fn(const void* key, int l, uint32_t seed):
    """
    Hash function for serialized terms (:py:class:`Buffer` objects)
    """
    return <size_t>spookyhash_64((<Buffer*>key).addr, (<Buffer*>key).sz, seed)


cdef size_t trp_hash_fn(const void* key, int l, uint32_t seed):
    """
    Hash function for sets of (serialized) triples.

    This function concatenates the literal terms of the triple as bytes
    and computes their hash.
    """
    trp = <BufferTriple*>key
    seed64 = <uint64_t>seed
    seed_dummy = seed64

    cdef sph.spookyhash_context ctx

    sph.spookyhash_context_init(&ctx, seed64, seed_dummy)
    sph.spookyhash_update(&ctx, trp.s.addr, trp.s.sz)
    sph.spookyhash_update(&ctx, trp.s.addr, trp.p.sz)
    sph.spookyhash_update(&ctx, trp.s.addr, trp.o.sz)
    sph.spookyhash_final(&ctx, &seed64, &seed_dummy)

    return <size_t>seed64


#cdef size_t trp_hash_fn(const void* key, int l, uint32_t seed):
#    """
#    Hash function for sets of (serialized) triples.
#
#    This function computes the hash of the concatenated pointer values in the
#    s, p, o members of the triple. The triple structure is treated as a byte
#    string. This is safe in spite of byte-wise struct evaluation being a
#    frowned-upon practice (due to padding issues), because it is assumed that
#    the input value is always the same type of structure.
#    """
#    return <size_t>spookyhash_64(key, l, seed)


#cdef size_t hash_ptr_passthrough(const void* key, int l, uint32_t seed):
#    """
#    No-op function that takes a pointer and does *not* hash it.
#
#    The pointer value is used as the "hash".
#    """
#    return <size_t>key


cdef inline bint lookup_none_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Dummy callback for queries with all parameters unbound.

    This function always returns ``True`` 
    """
    return True


cdef inline bint lookup_s_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Lookup callback compare function for a given ``s`` in a triple.

    The function returns ``True`` if ``t1`` matches the first term.

    ``t2`` is not used and is declared only for compatibility with the
    other interchangeable functions.
    """
    return not term_cmp_fn(t1, trp[0].s)


cdef inline bint lookup_p_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Lookup callback compare function for a given ``p`` in a triple.
    """
    return not term_cmp_fn(t1, trp[0].p)


cdef inline bint lookup_o_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Lookup callback compare function for a given ``o`` in a triple.
    """
    return not term_cmp_fn(t1, trp[0].o)


cdef inline bint lookup_sp_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Lookup callback compare function for a given ``s`` and ``p`` pair.
    """
    return (
            not term_cmp_fn(t1, trp[0].s)
            and not term_cmp_fn(t2, trp[0].p))


cdef inline bint lookup_so_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Lookup callback compare function for a given ``s`` and ``o`` pair.
    """
    return (
            not term_cmp_fn(t1, trp[0].s)
            and not term_cmp_fn(t2, trp[0].o))


cdef inline bint lookup_po_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    """
    Lookup callback compare function for a given ``p`` and ``o`` pair.
    """
    return (
            not term_cmp_fn(t1, trp[0].p)
            and not term_cmp_fn(t2, trp[0].o))


## LOOKUP CALLBACK FUNCTIONS

cdef inline void add_trp_callback(
    graph.SimpleGraph gr, const BufferTriple* trp, void* ctx
):
    """
    Add a triple to a graph as a result of a lookup callback.
    """
    gr.add_triple(trp, True)


cdef inline void del_trp_callback(
    graph.SimpleGraph gr, const BufferTriple* trp, void* ctx
):
    """
    Remove a triple from a graph as a result of a lookup callback.
    """
    #logger.info('removing triple: {} {} {}'.format(
        #buffer_dump(trp.s), buffer_dump(trp.p), buffer_dump(trp.o)
    #))
    gr.remove_triple(trp)


