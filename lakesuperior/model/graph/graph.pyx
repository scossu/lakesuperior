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
from lakesuperior.cy_include.hashset cimport (
    CC_OK,
    HashSet, HashSetConf, HashSetIter, TableEntry,
    hashset_add, hashset_conf_init, hashset_iter_init, hashset_iter_next,
    hashset_new_conf, hashtable_hash_ptr, hashset_size,
    get_table_index,
)
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


def use_data(fn):
    """
    Decorator to indicate that a set operation between two SimpleGraph
    instances should use the ``data`` property of the second term. The second
    term can also be a simple set.
    """
    @wraps(fn)
    def _wrapper(self, other):
        if isinstance(other, SimpleGraph):
            other = other.data
    return _wrapper


cdef bint term_cmp_fn(void* key1, void* key2):
    """
    Compare function for two Buffer objects.
    """
    b1 = <Buffer *>key1
    b2 = <Buffer *>key2

    if b1.sz != b2.sz:
        return False

    #print('Term A:')
    #print((<unsigned char *>b1.addr)[:b1.sz])
    #print('Term b:')
    #print((<unsigned char *>b2.addr)[:b2.sz])
    cdef int cmp = memcmp(b1.addr, b2.addr, b1.sz)
    logger.info(f'term memcmp: {cmp}')
    return cmp == 0


cdef bint triple_cmp_fn(void* key1, void* key2):
    """
    Compare function for two triples in a CAlg set.

    Here, pointers to terms are compared for s, p, o. The pointers should be
    guaranteed to point to unique values (i.e. no two pointers have the same
    term value within a graph).
    """
    t1 = <BufferTriple *>key1
    t2 = <BufferTriple *>key2

    return(
            t1.s.addr == t2.s.addr and
            t1.p.addr == t2.p.addr and
            t1.o.addr == t2.o.addr)


cdef size_t trp_hash_fn(void* key, int l, uint32_t seed):
    """
    Hash function for sets of (serialized) triples.

    This function computes the hash of the concatenated pointer values in the
    s, p, o members of the triple. The triple structure is treated as a byte
    string. This is safe in spite of byte-wise struct evaluation being a
    frowned-upon practice (due to padding issues), because it is assumed that
    the input value is always the same type of structure.
    """
    return <size_t>spookyhash_64(key, l, seed)


cdef size_t hash_ptr_passthrough(void* key, int l, uint32_t seed):
    """
    No-op function that takes a pointer and does *not* hash it.

    The pointer value is used as the "hash".
    """
    return <size_t>key


cdef inline bint lookup_none_cmp_fn(
        const BufferTriple *trp, const Buffer *t1, const Buffer *t2):
    """
    Dummy callback for queries with all parameters unbound.

    This function always returns ``True`` 
    """
    return True


cdef inline bint lookup_s_cmp_fn(BufferTriple *trp, Buffer *t1, Buffer *t2):
    """
    Lookup callback compare function for a given s in a triple.

    The function returns ``True`` if ``t1`` matches the first term.

    ``t2`` is not used and is declared only for compatibility with the
    other interchangeable functions.
    """
    return term_cmp_fn(t1, trp[0].s)


cdef inline bint lookup_p_cmp_fn(BufferTriple *trp, Buffer *t1, Buffer *t2):
    return term_cmp_fn(t1, trp[0].p)


cdef inline bint lookup_o_cmp_fn(BufferTriple *trp, Buffer *t1, Buffer *t2):
    return term_cmp_fn(t1, trp[0].o)


cdef inline bint lookup_sp_cmp_fn(BufferTriple *trp, Buffer *t1, Buffer *t2):
    return (
            term_cmp_fn(t1, trp[0].s)
            and term_cmp_fn(t2, trp[0].p))


cdef inline bint lookup_so_cmp_fn(BufferTriple *trp, Buffer *t1, Buffer *t2):
    return (
            term_cmp_fn(t1, trp[0].s)
            and term_cmp_fn(t2, trp[0].o))


cdef inline bint lookup_po_cmp_fn(BufferTriple *trp, Buffer *t1, Buffer *t2):
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
            self, Keyset keyset=None, store=None, set data=set()):
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
        hashset_conf_init(&self._terms_conf)
        self._terms_conf.load_factor = 0.85
        self._terms_conf.hash = hash_ptr_passthrough # spookyhash_64?
        self._terms_conf.hash_seed = term_hash_seed32
        self._terms_conf.key_compare = term_cmp_fn
        self._terms_conf.key_length = sizeof(void*)

        hashset_conf_init(&self._trp_conf)
        self._trp_conf.load_factor = 0.75
        self._trp_conf.hash = hash_ptr_passthrough # spookyhash_64?
        self._trp_conf.hash_seed = term_hash_seed32
        self._terms_conf.key_compare = triple_cmp_fn
        self._terms_conf.key_length = sizeof(void*)

        self.store = store or env.app_globals.rdf_store
        hashset_new_conf(&self._terms_conf, &self._terms)
        hashset_new_conf(&self._trp_conf, &self._triples)
        self._pool = Pool()

        cdef:
            size_t i = 0
            TripleKey spok
            term.Buffer pk_t

        # Initialize empty data set.
        if keyset:
            # Populate with triples extracted from provided key set.
            self._data_from_keyset(keyset)
        elif data is not None:
            # Populate with provided Python set.
            for s, p, o in data:
                self._add_from_rdflib(s, p, o)


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
        return self._data_as_set()


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


    cdef inline void _add_from_spok(self, const TripleKey spok) except *:
        """
        Add a triple from a TripleKey of term keys.
        """
        cdef:
            SPOBuffer s_spo
            BufferTriple trp

        s_spo = <SPOBuffer>self._pool.alloc(3, sizeof(Buffer))

        self.store.lookup_term(spok, s_spo)
        self.store.lookup_term(spok + KLEN, s_spo + 1)
        self.store.lookup_term(spok + DBL_KLEN, s_spo + 2)

        self._add_triple(s_spo, s_spo + 1, s_spo + 2)


    cdef inline void _add_triple(
        self, BufferPtr ss, BufferPtr sp, BufferPtr so
    ) except *:
        """
        Add a triple from 3 (TPL) serialized terms.

        Each of the terms is added to the term set if not existing. The triple
        also is only added if not existing.
        """
        trp = <BufferTriple *>self._pool.alloc(1, sizeof(BufferTriple))

        logger.info('Inserting terms.')
        logger.info(f'ss addr: {<unsigned long>ss.addr}')
        logger.info(f'ss sz: {ss.sz}')
        #logger.info('ss:')
        #logger.info((<unsigned char *>ss.addr)[:ss.sz])
        logger.info('Insert ss @:')
        print(<unsigned long>ss)
        self._add_or_get_term(&ss)
        logger.info('Now ss is @:')
        print(<unsigned long>ss)
        logger.info('Insert sp')
        self._add_or_get_term(&sp)
        logger.info('Insert so')
        self._add_or_get_term(&so)
        logger.info('inserted terms.')
        cdef size_t terms_sz = hashset_size(self._terms)
        logger.info('Terms set size: {terms_sz}')

        #cdef HashSetIter ti
        #cdef Buffer *t
        #hashset_iter_init(&ti, self._terms)
        #while calg.set_iter_has_more(&ti):
        #    t = <Buffer *>calg.set_iter_next(&ti)

        trp.s = ss
        trp.p = sp
        trp.o = so

        r = hashset_add(self._triples, trp)
        print('Insert triple result:')
        print(r)

        #cdef BufferTriple *tt
        #calg.set_iterate(self._triples, &ti)
        #while calg.set_iter_has_more(&ti):
        #    tt = <BufferTriple *>calg.set_iter_next(&ti)


    cdef int _add_or_get_term(self, Buffer **data) except -1:
        """
        Insert a term in the terms set, or get one that already exists.

        If the new term is inserted, its address is stored in the memory pool
        and persists with the :py:class:`SimpleGraph` instance carrying it.
        Otherwise, the overwritten term is garbage collected as soon as the
        calling function exits.

        The return value gives an indication of whether the term was added or
        not.
        """
        cdef TableEntry *entry

        table = self._terms.table

        entry = table.buckets[get_table_index(table, data[0].addr)]

        while entry:
            if table.key_cmp(data[0].addr, entry.key) == 0:
                # If the term is found, assign the address of entry.key
                # to the data parameter.
                data[0] = <Buffer *>entry.key
                return 1
            entry = entry.next

        # If the term is not found, add it.
        # TODO This is inefficient because it searches for the term again.
        # TODO It would be best to break down the hashset_add function and
        # TODO remove the check.
        return hashset_add(self._terms, data[0])


    cdef set _data_as_set(self):
        """
        Convert triple data to a Python set.

        :rtype: set
        """
        cdef:
            HashSetIter ti
            BufferTriple *trp
            term.Term s, p, o

        graph_set = set()

        hashset_iter_init(&ti, self._triples)
        while hashset_iter_next(&ti, &trp) == CC_OK:
            if trp == NULL:
                logger.warn('Triple is NULL!')
                break

            graph_set.add((
                term.deserialize_to_rdflib(trp.s),
                term.deserialize_to_rdflib(trp.p),
                term.deserialize_to_rdflib(trp.o),
            ))

        return graph_set


    # Basic set operations.

    def add(self, triple):
        """ Add one triple to the graph. """
        ss = <Buffer *>self._pool.alloc(1, sizeof(Buffer))
        sp = <Buffer *>self._pool.alloc(1, sizeof(Buffer))
        so = <Buffer *>self._pool.alloc(1, sizeof(Buffer))

        s, p, o = triple

        term.serialize_from_rdflib(s, ss, self._pool)
        term.serialize_from_rdflib(p, sp, self._pool)
        term.serialize_from_rdflib(o, so, self._pool)

        self._add_triple(ss, sp, so)


    def remove(self, item):
        """
        Remove one item from the graph.

        :param tuple item: A 3-tuple of RDFlib terms. Only exact terms, i.e.
            wildcards are not accepted.
        """
        self.data.remove(item)


    def __len__(self):
        """ Number of triples in the graph. """
        #return calg.set_num_entries(self._triples)
        return len(self.data)


    @use_data
    def __eq__(self, other):
        """ Equality operator between ``SimpleGraph`` instances. """
        return self.data == other


    def __repr__(self):
        """
        String representation of the graph.

        It provides the number of triples in the graph and memory address of
            the instance.
        """
        return (f'<{self.__class__.__name__} @{hex(id(self))} '
            f'length={len(self.data)}>')


    def __str__(self):
        """ String dump of the graph triples. """
        return str(self.data)


    @use_data
    def __sub__(self, other):
        """ Set subtraction. """
        return self.data - other


    @use_data
    def __isub__(self, other):
        """ In-place set subtraction. """
        self.data -= other
        return self

    @use_data
    def __and__(self, other):
        """ Set intersection. """
        return self.data & other


    @use_data
    def __iand__(self, other):
        """ In-place set intersection. """
        self.data &= other
        return self

    @use_data
    def __or__(self, other):
        """ Set union. """
        return self.data | other


    @use_data
    def __ior__(self, other):
        """ In-place set union. """
        self.data |= other
        return self

    @use_data
    def __xor__(self, other):
        """ Set exclusive intersection (XOR). """
        return self.data ^ other


    @use_data
    def __ixor__(self, other):
        """ In-place set exclusive intersection (XOR). """
        self.data ^= other
        return self


    def __contains__(self, item):
        """
        Whether the graph contains a triple.

        :rtype: boolean
        """
        return item in self.data


    def __iter__(self):
        """ Graph iterator. It iterates over the set triples. """
        return self.data.__iter__()


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
        self.add(trp)


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
            BufferTriple trp
            BufferTriple *trp_p
            HashSetIter ti
            const Buffer t1
            const Buffer t2
            lookup_fn_t fn

        res = set()

        # Decide comparison logic outside the loop.
        if s is not None and p is not None and o is not None:
            # Return immediately if 3-term match is requested.
            term.serialize_from_rdflib(s, trp.s)
            term.serialize_from_rdflib(p, trp.p)
            term.serialize_from_rdflib(o, trp.o)

            if hashset_contains(self._triples, &trp):
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
        hashset_iter_init(&ti, self._triples)
        while hashset_iter_next(&ti, &trp_p) == CC_OK:
            if fn(trp_p, &t1, &t2):
                res.add((
                    term.deserialize_to_rdflib(trp_p[0].s),
                    term.deserialize_to_rdflib(trp_p[0].p),
                    term.deserialize_to_rdflib(trp_p[0].o),
                ))

        return res


    cpdef set terms(self, str type):
        """
        Get all terms of a type: subject, predicate or object.

        :param str type: One of ``s``, ``p`` or ``o``.
        """
        i = 'spo'.index(type)
        return {r[i] for r in self.data}



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
    def __init__(self, str uri, *args, **kwargs):
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
        super().__init__(*args, **kwargs)

        self.uri = uri


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

    @use_data
    def __sub__(self, other):
        """
        Set difference. This creates a new Imr with the same subject URI.
        """
        return self.__class__(uri=self.uri, data=self.data - other)

    @use_data
    def __and__(self, other):
        """
        Set intersection. This creates a new Imr with the same subject URI.
        """
        return self.__class__(uri=self.uri, data=self.data & other)

    @use_data
    def __or__(self, other):
        """
        Set union. This creates a new Imr with the same subject URI.
        """
        return self.__class__(uri=self.uri, data=self.data | other)

    @use_data
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




