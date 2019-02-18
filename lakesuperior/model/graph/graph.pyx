import logging

from functools import wraps

from rdflib import Graph
from rdflib.term import Node

from lakesuperior import env

from libc.stdint cimport uint32_t, uint64_t
from libc.string cimport memcmp, memcpy
from libc.stdlib cimport free

from cymem.cymem cimport Pool

from lakesuperior.cy_include cimport cylmdb as lmdb
from lakesuperior.cy_include cimport collections as cc
from lakesuperior.cy_include cimport spookyhash as sph
from lakesuperior.model.graph cimport term
from lakesuperior.store.ldp_rs.lmdb_triplestore cimport (
        KLEN, DBL_KLEN, TRP_KLEN, TripleKey)
from lakesuperior.model.structures.hash cimport term_hash_seed32
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph.triple cimport BufferTriple
from lakesuperior.model.structures.hash cimport hash64

cdef extern from 'spookyhash_api.h':
    uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed)

logger = logging.getLogger(__name__)


cdef int term_cmp_fn(const void* key1, const void* key2):
    """
    Compare function for two Buffer objects.

    :rtype: int
    :return: 0 if the byte streams are the same, another integer otherwise.
    """
    b1 = <Buffer *>key1
    b2 = <Buffer *>key2

    if b1.sz != b2.sz:
        logger.info(f'Sizes differ: {b1.sz} != {b2.sz}. Return 1.')
        return 1

    cdef int cmp = memcmp(b1.addr, b2.addr, b1.sz)
    logger.info(f'term memcmp: {cmp}')
    return cmp


cdef int trp_lit_cmp_fn(const void* key1, const void* key2):
    """
    Compare function for two triples in a set.

    s, p, o byte data are compared literally.

    :rtype: int
    :return: 0 if all three terms point to byte-wise identical data in both
        triples.
    """
    t1 = <BufferTriple *>key1
    t2 = <BufferTriple *>key2
    #print('Comparing terms: {} {} {}'.format(
    #    (<unsigned char*>t1.s.addr)[:t1.s.sz],
    #    (<unsigned char*>t1.p.addr)[:t1.p.sz],
    #    (<unsigned char*>t1.o.addr)[:t1.o.sz]
    #))
    #print('With:            {} {} {}'.format(
    #    (<unsigned char*>t2.s.addr)[:t2.s.sz],
    #    (<unsigned char*>t2.p.addr)[:t2.p.sz],
    #    (<unsigned char*>t2.o.addr)[:t2.o.sz]
    #))

    diff = (
        term_cmp_fn(t1.o, t2.o) or
        term_cmp_fn(t1.s, t2.s) or
        term_cmp_fn(t1.p, t2.p)
    )

    logger.info(f'Triples match: {not(diff)}')
    return diff


cdef int trp_cmp_fn(const void* key1, const void* key2):
    """
    Compare function for two triples in a set.

    Here, pointers to terms are compared for s, p, o. The pointers should be
    guaranteed to point to unique values (i.e. no two pointers have the same
    term value within a graph).

    :rtype: int
    :return: 0 if the addresses of all terms are the same, 1 otherwise.
    """
    t1 = <BufferTriple *>key1
    t2 = <BufferTriple *>key2
    #print('Comparing terms: {} {} {}'.format(
    #    (<unsigned char*>t1.s.addr)[:t1.s.sz],
    #    (<unsigned char*>t1.p.addr)[:t1.p.sz],
    #    (<unsigned char*>t1.o.addr)[:t1.o.sz]
    #))
    #print('With:            {} {} {}'.format(
    #    (<unsigned char*>t2.s.addr)[:t2.s.sz],
    #    (<unsigned char*>t2.p.addr)[:t2.p.sz],
    #    (<unsigned char*>t2.o.addr)[:t2.o.sz]
    #))
    #print('Comparing addresses: <0x{:02x}> <0x{:02x}> <0x{:02x}>'.format(
    #    <size_t>t1.s, <size_t>t1.p, <size_t>t1.o))
    #print('With:                <0x{:02x}> <0x{:02x}> <0x{:02x}>'.format(
    #    <size_t>t2.s, <size_t>t2.p, <size_t>t2.o))

    cdef int is_not_equal = (
        t1.s.addr != t2.s.addr or
        t1.p.addr != t2.p.addr or
        t1.o.addr != t2.o.addr
    )

    logger.info(f'Triples match: {not(is_not_equal)}')
    return is_not_equal


cdef bint graph_eq_fn(SimpleGraph g1, SimpleGraph g2):
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


cdef size_t trp_lit_hash_fn(const void* key, int l, uint32_t seed):
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


cdef size_t trp_hash_fn(const void* key, int l, uint32_t seed):
    """
    Hash function for sets of (serialized) triples.

    This function computes the hash of the concatenated pointer values in the
    s, p, o members of the triple. The triple structure is treated as a byte
    string. This is safe in spite of byte-wise struct evaluation being a
    frowned-upon practice (due to padding issues), because it is assumed that
    the input value is always the same type of structure.
    """
    return <size_t>spookyhash_64(key, l, seed)


cdef size_t hash_ptr_passthrough(const void* key, int l, uint32_t seed):
    """
    No-op function that takes a pointer and does *not* hash it.

    The pointer value is used as the "hash".
    """
    return <size_t>key


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
    Lookup callback compare function for a given s in a triple.

    The function returns ``True`` if ``t1`` matches the first term.

    ``t2`` is not used and is declared only for compatibility with the
    other interchangeable functions.
    """
    return term_cmp_fn(t1, trp[0].s)


cdef inline bint lookup_p_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    return term_cmp_fn(t1, trp[0].p)


cdef inline bint lookup_o_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    return term_cmp_fn(t1, trp[0].o)


cdef inline bint lookup_sp_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    return (
            term_cmp_fn(t1, trp[0].s)
            and term_cmp_fn(t2, trp[0].p))


cdef inline bint lookup_so_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    return (
            term_cmp_fn(t1, trp[0].s)
            and term_cmp_fn(t2, trp[0].o))


cdef inline bint lookup_po_cmp_fn(
    const BufferTriple *trp, const Buffer *t1, const Buffer *t2
):
    return (
            term_cmp_fn(t1, trp[0].p)
            and term_cmp_fn(t2, trp[0].o))




cdef class SimpleGraph:
    """
    Fast and simple implementation of a graph.

    Most functions should mimic RDFLib's graph with less overhead. It uses
    the same funny but functional slicing notation. No lookup functions within
    the graph are available at this time.

    Instances of this class hold a set of
    :py:class:`~lakesuperior.store.ldp_rs.term.Term` structures that stores
    unique terms within the graph, and a set of
    :py:class:`~lakesuperior.store.ldp_rs.triple.Triple` structures referencing
    those terms. Therefore, no data duplication occurs and the storage is quite
    sparse.

    A graph can be instantiated from a store lookup.

    A SimpleGraph can also be obtained from a
    :py:class:`lakesuperior.store.keyset.Keyset` which is convenient bacause
    a Keyset can be obtained very efficiently from querying a store, then also
    very efficiently filtered and eventually converted into a set of meaningful
    terms.

    An instance of this class can also be converted to and from a
    ``rdflib.Graph`` instance. TODO verify that this frees Cython pointers.
    """

    def __cinit__(
            self, Keyset keyset=None, store=None, set data=set(), *args, **kwargs):
        """
        Initialize the graph with pre-existing data or by looking up a store.

        One of ``keyset``, or ``data`` can be provided. If more than
        one of these is provided, precedence is given in the mentioned order.
        If none of them is specified, an empty graph is initialized.

        :param rdflib.URIRef uri: The graph URI.
            This will serve as the subject for some queries.
        :param Keyset keyset: Keyset to create the graph from. Keys will be
            converted to set elements.
        :param lakesuperior.store.ldp_rs.LmdbTripleStore store: store to
            look up the keyset. Only used if ``keyset`` is specified. If not
            set, the environment store is used.
        :param set data: Initial data as a set of 3-tuples of RDFLib terms.
        :param tuple lookup: tuple of a 3-tuple of lookup terms, and a context.
            E.g. ``((URIRef('urn:ns:a'), None, None), URIRef('urn:ns:ctx'))``.
            Any and all elements may be ``None``.
        :param lmdbStore store: the store to look data up.
        """
        cdef:
            cc.HashSetConf terms_conf, trp_conf

        self.term_cmp_fn = &term_cmp_fn
        self.trp_cmp_fn = &trp_lit_cmp_fn

        cc.hashset_conf_init(&terms_conf)
        terms_conf.load_factor = 0.85
        terms_conf.hash = &term_hash_fn
        terms_conf.hash_seed = term_hash_seed32
        terms_conf.key_compare = self.term_cmp_fn
        terms_conf.key_length = sizeof(Buffer*)

        cc.hashset_conf_init(&trp_conf)
        trp_conf.load_factor = 0.75
        trp_conf.hash = &trp_lit_hash_fn
        trp_conf.hash_seed = term_hash_seed32
        trp_conf.key_compare = self.trp_cmp_fn
        trp_conf.key_length = sizeof(BufferTriple)

        cc.hashset_new_conf(&terms_conf, &self._terms)
        cc.hashset_new_conf(&trp_conf, &self._triples)

        self.store = store or env.app_globals.rdf_store
        self._pool = Pool()

        # Initialize empty data set.
        if keyset:
            # Populate with triples extracted from provided key set.
            self._data_from_keyset(keyset)
        elif data:
            # Populate with provided Python set.
            self.add(data)


    def __dealloc__(self):
        """
        Free the triple pointers. TODO use a Cymem pool
        """
        free(self._triples)
        free(self._terms)


    @property
    def data(self):
        """
        Triple data as a Python set.

        :rtype: set
        """
        return self._to_pyset()

    @property
    def terms(self):
        return self._get_terms()


    # # # BASIC SET OPERATIONS # # #

    cpdef SimpleGraph union(self, SimpleGraph other):
        """
        Perform set union resulting in a new SimpleGraph instance.

        TODO Allow union of multiple graphs at a time.

        :param SimpleGraph other: The other graph to merge.

        :rtype: SimpleGraph
        :return: A new SimpleGraph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it
            SimpleGraph new_gr = SimpleGraph()
            BufferTriple *trp

        new_gr.store = self.store

        for gr in (self, other):
            cc.hashset_iter_init(&it, gr._triples)
            while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
                bt = <BufferTriple*>cur
                new_gr._add_triple(bt)

        return new_gr


    cdef void ip_union(self, SimpleGraph other) except *:
        """
        Perform an in-place set union that adds triples to this instance

        TODO Allow union of multiple graphs at a time.

        :param SimpleGraph other: The other graph to merge.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it

        cc.hashset_iter_init(&it, other._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            self._add_triple(bt)


    cpdef SimpleGraph intersection(self, SimpleGraph other):
        """
        Graph intersection.

        :param SimpleGraph other: The other graph to intersect.

        :rtype: SimpleGraph
        :return: A new SimpleGraph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it
            SimpleGraph new_gr = SimpleGraph()

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            #print('Checking: <0x{:02x}> <0x{:02x}> <0x{:02x}>'.format(
            #    <size_t>bt.s, <size_t>bt.p, <size_t>bt.o))
            if other._trp_contains(bt):
                #print('Adding.')
                new_gr._add_triple(bt)

        return new_gr


    cdef void ip_intersection(self, SimpleGraph other) except *:
        """
        In-place graph intersection.

        Triples in common with another graph are removed from the current one.

        :param SimpleGraph other: The other graph to intersect.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not other._trp_contains(bt):
                self._remove_triple(bt)


    cpdef SimpleGraph xor(self, SimpleGraph other):
        """
        Graph Exclusive disjunction (XOR).

        :param SimpleGraph other: The other graph to perform XOR with.

        :rtype: SimpleGraph
        :return: A new SimpleGraph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it
            SimpleGraph new_gr = SimpleGraph()
            BufferTriple* bt

        # Add triples in this and not in other.
        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not other._trp_contains(bt):
                new_gr._add_triple(bt)

        # Other way around.
        cc.hashset_iter_init(&it, other._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not self._trp_contains(bt):
                new_gr._add_triple(bt)

        return new_gr


    cdef void ip_xor(self, SimpleGraph other) except *:
        """
        In-place graph XOR.

        Triples in common with another graph are removed from the current one,
        and triples not in common will be added from the other one.

        :param SimpleGraph other: The other graph to perform XOR with.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it
            # TODO This could be more efficient to stash values in a simple
            # array, but how urgent is it to improve an in-place XOR?
            SimpleGraph tmp = SimpleGraph()

        # Add *to the tmp graph* triples in other graph and not in this graph.
        cc.hashset_iter_init(&it, other._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not self._trp_contains(bt):
                tmp._add_triple(bt)

        # Remove triples in common.
        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if other._trp_contains(bt):
                print(self._remove_triple(bt))

        self |= tmp



    cdef void _data_from_lookup(self, tuple trp_ptn, ctx=None) except *:
        """
        Look up triples in the triplestore and load them into ``data``.

        :param tuple lookup: 3-tuple of RDFlib terms or ``None``.
        :param LmdbTriplestore store: Reference to a LMDB triplestore. This
            is normally set to ``lakesuperior.env.app_globals.rdf_store``.
        """
        cdef:
            size_t i
            unsigned char spok[TRP_KLEN]

        with self.store.txn_ctx():
            keyset = self.store.triple_keys(trp_ptn, ctx)
            self.data_from_keyset(keyset)



    cdef void _data_from_keyset(self, Keyset data) except *:
        """Populate a graph from a Keyset."""
        cdef TripleKey spok

        while data.next(spok):
            self._add_from_spok(spok)


    cdef inline void _add_from_spok(self, TripleKey spok) except *:
        """
        Add a triple from a TripleKey of term keys.
        """
        s_spo = <SPOBuffer>self._pool.alloc(3, sizeof(Buffer))

        self.store.lookup_term(spok, s_spo)
        self.store.lookup_term(spok + KLEN, s_spo + 1)
        self.store.lookup_term(spok + DBL_KLEN, s_spo + 2)

        trp = <BufferTriple *>self._pool.alloc(1, sizeof(BufferTriple))
        trp.s = s_spo
        trp.p = s_spo + 1
        trp.o = s_spo + 2

        self._add_triple(trp)


    cdef inline void _add_triple(self, BufferTriple* trp) except *:
        """
        Add a triple from 3 (TPL) serialized terms.

        Each of the terms is added to the term set if not existing. The triple
        also is only added if not existing.
        """
        logger.info('Inserting terms.')
        cc.hashset_add(self._terms, trp.s)
        cc.hashset_add(self._terms, trp.p)
        cc.hashset_add(self._terms, trp.o)
        logger.info('inserted terms.')
        logger.info(f'Terms set size: {cc.hashset_size(self._terms)}')

        cdef size_t trp_sz = cc.hashset_size(self._triples)
        logger.info(f'Triples set size before adding: {trp_sz}')

        r = cc.hashset_add(self._triples, trp)
        #print('Insert triple result:')
        #print(r)

        trp_sz = cc.hashset_size(self._triples)
        logger.info(f'Triples set size after adding: {trp_sz}')

        cdef:
            cc.HashSetIter ti
            void *cur


    cdef int _remove_triple(self, BufferTriple* btrp) except -1:
        """
        Remove one triple from the graph.
        """
        return cc.hashset_remove(self._triples, btrp, NULL)


    cdef bint _trp_contains(self, BufferTriple* btrp):
        cdef:
            cc.HashSetIter it
            void* cur

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            if self.trp_cmp_fn(cur, btrp) == 0:
                return True
        return False


    cdef _get_terms(self):
        """
        Get all terms in the graph.
        """
        cdef:
            cc.HashSetIter it
            void *cur

        terms = [] # This is intentionally a list to spot issues with the set.

        cc.hashset_iter_init(&it, self._terms)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            s_term = <Buffer*>cur
            terms.append((f'0x{<size_t>cur:02x}', term.deserialize_to_rdflib(s_term)))

        return terms


    cdef set _to_pyset(self):
        """
        Convert triple data to a Python set.

        :rtype: set
        """
        cdef:
            void *void_p
            cc.HashSetIter ti
            term.Term s, p, o

        graph_set = set()
        # Looping over an empty HashSet results in a segfault. Exit early in
        # that case.
        #if not cc.hashset_size(self._triples):
        #    return graph_set

        cc.hashset_iter_init(&ti, self._triples)
        while cc.hashset_iter_next(&ti, &void_p) != cc.CC_ITER_END:
            if void_p == NULL:
                logger.warn('Triple is NULL!')
                break

            trp = <BufferTriple *>void_p
            graph_set.add((
                term.deserialize_to_rdflib(trp.s),
                term.deserialize_to_rdflib(trp.p),
                term.deserialize_to_rdflib(trp.o),
            ))

        return graph_set


    # Basic set operations.

    def add(self, trp):
        """
        Add triples to the graph.

        :param iterable triples: Set, list or tuple of 3-tuple triples.
        """
        cdef size_t cur = 0, trp_cur = 0

        trp_ct = len(trp)
        term_buf = <Buffer*>self._pool.alloc(3 * trp_ct, sizeof(Buffer))
        trp_buf = <BufferTriple*>self._pool.alloc(trp_ct, sizeof(BufferTriple))

        for s, p, o in trp:
            term.serialize_from_rdflib(s, term_buf + cur, self._pool)
            term.serialize_from_rdflib(p, term_buf + cur + 1, self._pool)
            term.serialize_from_rdflib(o, term_buf + cur + 2, self._pool)

            (trp_buf + trp_cur).s = term_buf + cur
            (trp_buf + trp_cur).p = term_buf + cur + 1
            (trp_buf + trp_cur).o = term_buf + cur + 2

            self._add_triple(trp_buf + trp_cur)

            trp_cur += 1
            cur += 3


    def len_terms(self):
        """ Number of triples in the graph. """
        return cc.hashset_size(self._terms)


    def remove(self, trp):
        """
        Remove one item from the graph.

        :param tuple item: A 3-tuple of RDFlib terms. Only exact terms, i.e.
            wildcards are not accepted.
        """
        cdef:
            Buffer ss, sp, so
            BufferTriple trp_buf

        term.serialize_from_rdflib(trp[0], &ss, self._pool)
        term.serialize_from_rdflib(trp[1], &sp, self._pool)
        term.serialize_from_rdflib(trp[2], &so, self._pool)

        trp_buf.s = &ss
        trp_buf.p = &sp
        trp_buf.o = &so

        self._remove_triple(&trp_buf)


    def __len__(self):
        """ Number of triples in the graph. """
        return cc.hashset_size(self._triples)


    def __eq__(self, other):
        """ Equality operator between ``SimpleGraph`` instances. """
        return graph_eq_fn(self, other)


    def __repr__(self):
        """
        String representation of the graph.

        It provides the number of triples in the graph and memory address of
            the instance.
        """
        return (
            f'<{self.__class__.__name__} @{hex(id(self))} '
            f'length={len(self.data)}>'
        )


    def __str__(self):
        """ String dump of the graph triples. """
        return str(self.data)


    def __sub__(self, other):
        """ Set subtraction. """
        return self.subtract(other)


    def __isub__(self, other):
        """ In-place set subtraction. """
        self.ip_subtract(other)
        return self

    def __and__(self, other):
        """ Set intersection. """
        return self.intersection(other)


    def __iand__(self, other):
        """ In-place set intersection. """
        self.ip_intersection(other)
        return self


    def __or__(self, other):
        """ Set union. """
        return self.union(other)


    def __ior__(self, other):
        """ In-place set union. """
        self.ip_union(other)
        return self


    def __xor__(self, other):
        """ Set exclusive disjunction (XOR). """
        return self.xor(other)


    def __ixor__(self, other):
        """ In-place set exclusive disjunction (XOR). """
        self.ip_xor(other)
        return self


    def __contains__(self, trp):
        """
        Whether the graph contains a triple.

        :rtype: boolean
        """
        cdef:
            Buffer ss, sp, so
            BufferTriple btrp

        btrp.s = &ss
        btrp.p = &sp
        btrp.o = &so

        s, p, o = trp
        term.serialize_from_rdflib(s, &ss)
        term.serialize_from_rdflib(p, &sp)
        term.serialize_from_rdflib(o, &so)

        return self._trp_contains(&btrp)


    def __iter__(self):
        """ Graph iterator. It iterates over the set triples. """
        raise NotImplementedError()


    # Slicing.

    def __getitem__(self, item):
        """
        Slicing function.

        It behaves similarly to `RDFLib graph slicing
        <https://rdflib.readthedocs.io/en/stable/utilities.html#slicing-graphs>`__
        """
        if isinstance(item, slice):
            s, p, o = item.start, item.stop, item.step
            return self._slice(s, p, o)
        else:
            raise TypeError(f'Wrong slice format: {item}.')


    cpdef void set(self, tuple trp) except *:
        """
        Set a single value for subject and predicate.

        Remove all triples matching ``s`` and ``p`` before adding ``s p o``.
        """
        if None in trp:
            raise ValueError(f'Invalid triple: {trp}')
        self.remove_triples((trp[0], trp[1], None))
        self.add((trp,))


    cpdef void remove_triples(self, pattern) except *:
        """
        Remove triples by pattern.

        The pattern used is similar to :py:meth:`LmdbTripleStore.delete`.
        """
        s, p, o = pattern
        for match in self.lookup(s, p, o):
            logger.debug(f'Removing from graph: {match}.')
            self.data.remove(match)


    cpdef object as_rdflib(self):
        """
        Return the data set as an RDFLib Graph.

        :rtype: rdflib.Graph
        """
        gr = Graph()
        for trp in self.data:
            gr.add(trp)

        return gr


    def _slice(self, s, p, o):
        """
        Return terms filtered by other terms.

        This behaves like the rdflib.Graph slicing policy.
        """
        _data = self.data

        logger.debug(f'Slicing graph by: {s}, {p}, {o}.')
        if s is None and p is None and o is None:
            return _data
        elif s is None and p is None:
            return {(r[0], r[1]) for r in _data if r[2] == o}
        elif s is None and o is None:
            return {(r[0], r[2]) for r in _data if r[1] == p}
        elif p is None and o is None:
            return {(r[1], r[2]) for r in _data if r[0] == s}
        elif s is None:
            return {r[0] for r in _data if r[1] == p and r[2] == o}
        elif p is None:
            return {r[1] for r in _data if r[0] == s and r[2] == o}
        elif o is None:
            return {r[2] for r in _data if r[0] == s and r[1] == p}
        else:
            # all given
            return (s,p,o) in _data


    def lookup(self, s, p, o):
        """
        Look up triples by a pattern.

        This function converts RDFLib terms into the serialized format stored
        in the graph's internal structure and compares them bytewise.

        Any and all of the lookup terms can be ``None``.
        """
        cdef:
            void *void_p
            BufferTriple trp
            BufferTriple *trp_p
            cc.HashSetIter ti
            Buffer t1
            Buffer t2
            lookup_fn_t fn

        res = set()

        # Decide comparison logic outside the loop.
        if s is not None and p is not None and o is not None:
            # Return immediately if 3-term match is requested.
            term.serialize_from_rdflib(s, trp.s, self._pool)
            term.serialize_from_rdflib(p, trp.p, self._pool)
            term.serialize_from_rdflib(o, trp.o, self._pool)

            if cc.hashset_contains(self._triples, &trp):
                res.add((s, p, o))

            return res

        elif s is not None:
            term.serialize_from_rdflib(s, &t1)
            if p is not None:
                fn = lookup_sp_cmp_fn
                term.serialize_from_rdflib(p, &t2)
            elif o is not None:
                fn = lookup_so_cmp_fn
                term.serialize_from_rdflib(o, &t2)
            else:
                fn = lookup_s_cmp_fn
        elif p is not None:
            term.serialize_from_rdflib(p, &t1)
            if o is not None:
                fn = lookup_po_cmp_fn
                term.serialize_from_rdflib(o, &t2)
            else:
                fn = lookup_p_cmp_fn
        elif o is not None:
            fn = lookup_o_cmp_fn
            term.serialize_from_rdflib(o, &t1)
        else:
            fn = lookup_none_cmp_fn

        # Iterate over serialized triples.
        cc.hashset_iter_init(&ti, self._triples)
        while cc.hashset_iter_next(&ti, &void_p) != cc.CC_ITER_END:
            if void_p == NULL:
                trp_p = <BufferTriple *>void_p
                res.add((
                    term.deserialize_to_rdflib(trp_p[0].s),
                    term.deserialize_to_rdflib(trp_p[0].p),
                    term.deserialize_to_rdflib(trp_p[0].o),
                ))

        return res


    #cpdef set terms(self, str type):
    #    """
    #    Get all terms of a type: subject, predicate or object.

    #    :param str type: One of ``s``, ``p`` or ``o``.
    #    """
    #    i = 'spo'.index(type)
    #    return {r[i] for r in self.data}



cdef class Imr(SimpleGraph):
    """
    In-memory resource data container.

    This is an extension of :py:class:`~SimpleGraph` that adds a subject URI to
    the data set and some convenience methods.

    An instance of this class can be converted to a ``rdflib.Resource``
    instance.

    Some set operations that produce a new object (``-``, ``|``, ``&``, ``^``)
    will create a new ``Imr`` instance with the same subject URI.
    """
    def __init__(self, uri, *args, **kwargs):
        """
        Initialize the graph with pre-existing data or by looking up a store.

        Either ``data``, or ``lookup`` *and* ``store``, can be provide.
        ``lookup`` and ``store`` have precedence. If none of them is specified,
        an empty graph is initialized.

        :param rdflib.URIRef uri: The graph URI.
            This will serve as the subject for some queries.
        :param set data: Initial data as a set of 3-tuples of RDFLib terms.
        :param tuple lookup: tuple of a 3-tuple of lookup terms, and a context.
            E.g. ``((URIRef('urn:ns:a'), None, None), URIRef('urn:ns:ctx'))``.
            Any and all elements may be ``None``.
        :param lmdbStore store: the store to look data up.
        """
        self.uri = str(uri)


    @property
    def identifier(self):
        """
        IMR URI. For compatibility with RDFLib Resource.

        :rtype: string
        """
        return self.uri


    @property
    def graph(self):
        """
        Return a SimpleGraph with the same data.

        :rtype: SimpleGraph
        """
        return SimpleGraph(self.data)


    def __repr__(self):
        """
        String representation of an Imr.

        This includes the subject URI, number of triples contained and the
        memory address of the instance.
        """
        return (f'<{self.__class__.__name__} @{hex(id(self))} uri={self.uri}, '
            f'length={len(self.data)}>')

    def __sub__(self, other):
        """
        Set difference. This creates a new Imr with the same subject URI.
        """
        return self.__class__(uri=self.uri, data=self.data - other)

    def __and__(self, other):
        """
        Set intersection. This creates a new Imr with the same subject URI.
        """
        return self.__class__(uri=self.uri, data=self.data & other)

    def __or__(self, other):
        """
        Set union. This creates a new Imr with the same subject URI.
        """
        return self.__class__(uri=self.uri, data=self.data | other)

    def __xor__(self, other):
        """
        Set exclusive OR (XOR). This creates a new Imr with the same subject
        URI.
        """
        return self.__class__(uri=self.uri, data=self.data ^ other)


    def __getitem__(self, item):
        """
        Supports slicing notation.
        """
        if isinstance(item, slice):
            s, p, o = item.start, item.stop, item.step
            return self._slice(s, p, o)

        elif isinstance(item, Node):
            # If a Node is given, return all values for that predicate.
            return {
                    r[2] for r in self.data
                    if r[0] == self.uri and r[1] == item}
        else:
            raise TypeError(f'Wrong slice format: {item}.')


    def value(self, p, strict=False):
        """
        Get an individual value.

        :param rdflib.termNode p: Predicate to search for.
        :param bool strict: If set to ``True`` the method raises an error if
            more than one value is found. If ``False`` (the default) only
            the first found result is returned.
        :rtype: rdflib.term.Node
        """
        values = self[p]

        if strict and len(values) > 1:
            raise RuntimeError('More than one value found for {}, {}.'.format(
                    self.uri, p))

        for ret in values:
            return ret

        return None


    cpdef as_rdflib(self):
        """
        Return the IMR as a RDFLib Resource.

        :rtype: rdflib.Resource
        """
        gr = Graph()
        for trp in self.data:
            gr.add(trp)

        return gr.resource(identifier=self.uri)




