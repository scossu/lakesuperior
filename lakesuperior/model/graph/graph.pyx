import logging

from functools import wraps

from rdflib import Graph, URIRef
from rdflib.term import Node

from lakesuperior import env

from libc.string cimport memcpy
from libc.stdlib cimport free

from cymem.cymem cimport Pool

cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.model.graph.callbacks as cb

from lakesuperior.model.base cimport Buffer, buffer_dump
from lakesuperior.model.structures.keyset import Keyset
from lakesuperior.model.graph cimport term
from lakesuperior.model.graph.triple cimport BufferTriple
from lakesuperior.model.structures.hash cimport term_hash_seed32

logger = logging.getLogger(__name__)


cdef class Graph(Keyset):
    """
    Fast and simple implementation of a graph.

    Most functions should mimic RDFLib's graph with less overhead. It uses
    the same funny but functional slicing notation.

    A Graph can be instantiated from a store lookup. This makes it
    possible to use a Keyset to perform initial filtering via identity by key,
    then the filtered Keyset can be converted into a set of meaningful terms.

    An instance of this class can also be converted to and from a
    ``rdflib.Graph`` instance.
    """

    def __cinit__(self, *args, str uri=None, set data=set(), **kwargs):
        """
        Initialize the graph, optionally with Python data.

        :param set data: Initial data as a set of 3-tuples of RDFLib terms.
        """

        self.pool = Pool()

        # Initialize empty data set.
        if data:
            # Populate with provided Python set.
            self.add(data)


    ## PROPERTIES ##

    @property
    def data(self):
        """
        Triple data as a Python set.

        :rtype: set
        """
        cdef TripleKey spok

        ret = set()

        self.seek()
        while self.get_next(&spok):
            ret.add((
                self.store.from_key(trp[0]),
                self.store.from_key(trp[1]),
                self.store.from_key(trp[2])
            ))

        return ret


    ## MAGIC METHODS ##

    def __len__(self):
        """ Number of triples in the graph. """
        return self._free_i


    def __eq__(self, other):
        """ Equality operator between ``Graph`` instances. """
        return len(self ^ other) == 0


    def __repr__(self):
        """
        String representation of the graph.

        It provides the number of triples in the graph and memory address of
            the instance.
        """
        return (
            f'<{self.__class__.__name__} @{hex(id(self))} '
            f'length={len(self)}>'
        )


    def __str__(self):
        """ String dump of the graph triples. """
        return str(self.data)


    def __add__(self, other):
        """ Alias for set-theoretical union. """
        return self.union_(other)


    def __iadd__(self, other):
        """ Alias for in-place set-theoretical union. """
        self.ip_union(other)
        return self


    def __sub__(self, other):
        """ Set-theoretical subtraction. """
        return self.subtraction(other)


    def __isub__(self, other):
        """ In-place set-theoretical subtraction. """
        self.ip_subtraction(other)
        return self

    def __and__(self, other):
        """ Set-theoretical intersection. """
        return self.intersection(other)


    def __iand__(self, other):
        """ In-place set-theoretical intersection. """
        self.ip_intersection(other)
        return self


    def __or__(self, other):
        """ Set-theoretical union. """
        return self.union_(other)


    def __ior__(self, other):
        """ In-place set-theoretical union. """
        self.ip_union(other)
        return self


    def __xor__(self, other):
        """ Set-theoretical exclusive disjunction (XOR). """
        return self.xor(other)


    def __ixor__(self, other):
        """ In-place set-theoretical exclusive disjunction (XOR). """
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

        return self.trp_contains(&btrp)


    def __iter__(self):
        """ Graph iterator. It iterates over the set triples. """
        yield from self.data


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


    def __hash__(self):
        return 23465


    ## BASIC PYTHON-ACCESSIBLE SET OPERATIONS ##

    def terms_by_type(self, type):
        """
        Get all terms of a type: subject, predicate or object.

        :param str type: One of ``s``, ``p`` or ``o``.
        """
        i = 'spo'.index(type)
        return {r[i] for r in self.data}


    def add_triples(self, trp):
        """
        Add triples to the graph.

        :param iterable triples: iterable of 3-tuple triples.
        """
        for s, p, o in triples:
            self.add([
                self.store.to_key(s),
                self.store.to_key(p),
                self.store.to_key(o),
            ])


    def len_terms(self):
        """ Number of terms in the graph. """
        return cc.hashset_size(self._terms)


    def remove(self, pattern):
        """
        Remove triples by pattern.

        The pattern used is similar to :py:meth:`LmdbTripleStore.delete`.
        """
        self._match_ptn_callback(
            pattern, self, cb.del_trp_callback, NULL
        )


    ## CYTHON-ACCESSIBLE BASIC METHODS ##

    cdef Graph empty_copy(self):
        """
        Create an empty copy carrying over some key properties.

        Override in subclasses to accommodate for different init properties.
        """
        return self.__class__()


    cpdef union_(self, Graph other):
        """
        Perform set union resulting in a new Graph instance.

        TODO Allow union of multiple graphs at a time.

        :param Graph other: The other graph to merge.

        :rtype: Graph
        :return: A new Graph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it
            BufferTriple *trp

        new_gr = self.empty_copy()

        for gr in (self, other):
            cc.hashset_iter_init(&it, gr._triples)
            while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
                bt = <BufferTriple*>cur
                new_gr.add_triple(bt, True)

        return new_gr


    cdef void ip_union(self, Graph other) except *:
        """
        Perform an in-place set union that adds triples to this instance

        TODO Allow union of multiple graphs at a time.

        :param Graph other: The other graph to merge.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it

        cc.hashset_iter_init(&it, other._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            self.add_triple(bt, True)


    cpdef intersection(self, Graph other):
        """
        Graph intersection.

        :param Graph other: The other graph to intersect.

        :rtype: Graph
        :return: A new Graph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it

        new_gr = self.empty_copy()

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if other.trp_contains(bt):
                new_gr.add_triple(bt, True)

        return new_gr


    cdef void ip_intersection(self, Graph other) except *:
        """
        In-place graph intersection.

        Triples not in common with another graph are removed from the current
        one.

        :param Graph other: The other graph to intersect.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not other.trp_contains(bt):
                self.remove_triple(bt)


    cpdef subtraction(self, Graph other):
        """
        Graph set-theoretical subtraction.

        Create a new graph with the triples of this graph minus the ones in
        common with the other graph.

        :param Graph other: The other graph to subtract to this.

        :rtype: Graph
        :return: A new Graph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it

        new_gr = self.empty_copy()

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not other.trp_contains(bt):
                new_gr.add_triple(bt, True)

        return new_gr


    cdef void ip_subtraction(self, Graph other) except *:
        """
        In-place graph subtraction.

        Triples in common with another graph are removed from the current one.

        :param Graph other: The other graph to intersect.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if other.trp_contains(bt):
                self.remove_triple(bt)


    cpdef xor(self, Graph other):
        """
        Graph Exclusive disjunction (XOR).

        :param Graph other: The other graph to perform XOR with.

        :rtype: Graph
        :return: A new Graph instance.
        """
        cdef:
            void *cur
            cc.HashSetIter it
            BufferTriple* bt

        new_gr = self.empty_copy()

        # Add triples in this and not in other.
        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not other.trp_contains(bt):
                new_gr.add_triple(bt, True)

        # Other way around.
        cc.hashset_iter_init(&it, other._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not self.trp_contains(bt):
                new_gr.add_triple(bt, True)

        return new_gr


    cdef void ip_xor(self, Graph other) except *:
        """
        In-place graph XOR.

        Triples in common with another graph are removed from the current one,
        and triples not in common will be added from the other one.

        :param Graph other: The other graph to perform XOR with.

        :rtype: void
        """
        cdef:
            void *cur
            cc.HashSetIter it
            # TODO This could be more efficient to stash values in a simple
            # array, but how urgent is it to improve an in-place XOR?
            Graph tmp = Graph()

        # Add *to the tmp graph* triples in other graph and not in this graph.
        cc.hashset_iter_init(&it, other._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if not self.trp_contains(bt):
                tmp.add_triple(bt)

        # Remove triples in common.
        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            bt = <BufferTriple*>cur
            if other.trp_contains(bt):
                self.remove_triple(bt)

        self |= tmp


    cdef inline BufferTriple* store_triple(self, const BufferTriple* strp):
        """
        Store triple data in the graph.

        Normally, raw data underlying the triple and terms are only referenced
        by pointers. If the destination data are garbage collected before the
        graph is, segfaults are bound to happen.

        This method copies the data to the graph's memory pool, so they are
        managed with the lifecycle of the graph.

        Note that this method stores items regardless of whether thwy are
        duplicate or not, so there may be some duplication.
        """
        cdef:
            BufferTriple* dtrp = <BufferTriple*>self.pool.alloc(
                1, sizeof(BufferTriple)
            )
            Buffer* spo = <Buffer*>self.pool.alloc(3, sizeof(Buffer))

        if not dtrp:
            raise MemoryError()
        if not spo:
            raise MemoryError()

        dtrp.s = spo
        dtrp.p = spo + 1
        dtrp.o = spo + 2

        spo[0].addr = self.pool.alloc(strp.s.sz, 1)
        spo[0].sz = strp.s.sz
        spo[1].addr = self.pool.alloc(strp.p.sz, 1)
        spo[1].sz = strp.p.sz
        spo[2].addr = self.pool.alloc(strp.o.sz, 1)
        spo[2].sz = strp.o.sz

        if not spo[0].addr or not spo[1].addr or not spo[2].addr:
            raise MemoryError()

        memcpy(dtrp.s.addr, strp.s.addr, strp.s.sz)
        memcpy(dtrp.p.addr, strp.p.addr, strp.p.sz)
        memcpy(dtrp.o.addr, strp.o.addr, strp.o.sz)

        return dtrp


    cdef bint trp_contains(self, const BufferTriple* btrp):
        cdef:
            cc.HashSetIter it
            void* cur

        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            if self.trp_cmp_fn(cur, btrp) == 0:
                return True
        return False


    cpdef void set(self, tuple trp) except *:
        """
        Set a single value for subject and predicate.

        Remove all triples matching ``s`` and ``p`` before adding ``s p o``.
        """
        if None in trp:
            raise ValueError(f'Invalid triple: {trp}')
        self.remove((trp[0], trp[1], None))
        self.add((trp,))


    def as_rdflib(self):
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
        # If no terms are unbound, check for containment.
        if s is not None and p is not None and o is not None: # s p o
            return (s, p, o) in self

        # If some terms are unbound, do a lookup.
        res = self.lookup((s, p, o))
        if s is not None:
            if p is not None: # s p ?
                return {r[2] for r in res}

            if o is not None: # s ? o
                return {r[1] for r in res}

            # s ? ?
            return {(r[1], r[2]) for r in res}

        if p is not None:
            if o is not None: # ? p o
                return {r[0] for r in res}

            # ? p ?
            return {(r[0], r[2]) for r in res}

        if o is not None: # ? ? o
            return {(r[0], r[1]) for r in res}

        # ? ? ?
        return res


    def lookup(self, pattern):
        """
        Look up triples by a pattern.

        This function converts RDFLib terms into the serialized format stored
        in the graph's internal structure and compares them bytewise.

        Any and all of the lookup terms msy be ``None``.

        :rtype: Graph
        "return: New Graph instance with matching triples.
        """
        cdef:
            void* cur
            BufferTriple trp
            Graph res_gr = Graph()

        self._match_ptn_callback(pattern, res_gr, cb.add_trp_callback, NULL)

        return res_gr


    cdef void _match_ptn_callback(
        self, pattern, Graph gr,
        lookup_callback_fn_t callback_fn, void* ctx=NULL
    ) except *:
        """
        Execute an arbitrary function on a list of triples matching a pattern.

        The arbitrary function is appied to each triple found in the current
        graph, and to a discrete graph that can be the current graph itself
        or a different one.
        """
        cdef:
            void* cur
            Buffer t1, t2
            Buffer ss, sp, so
            BufferTriple trp
            BufferTriple* trp_p
            lookup_fn_t cmp_fn
            cc.HashSetIter it

        s, p, o = pattern

        # Decide comparison logic outside the loop.
        if s is not None and p is not None and o is not None:
            # Shortcut for 3-term match.
            trp.s = &ss
            trp.p = &sp
            trp.o = &so
            term.serialize_from_rdflib(s, trp.s, self.pool)
            term.serialize_from_rdflib(p, trp.p, self.pool)
            term.serialize_from_rdflib(o, trp.o, self.pool)

            if cc.hashset_contains(self._triples, &trp):
                callback_fn(gr, &trp, ctx)
                return

        if s is not None:
            term.serialize_from_rdflib(s, &t1)
            if p is not None:
                cmp_fn = cb.lookup_sp_cmp_fn
                term.serialize_from_rdflib(p, &t2)
            elif o is not None:
                cmp_fn = cb.lookup_so_cmp_fn
                term.serialize_from_rdflib(o, &t2)
            else:
                cmp_fn = cb.lookup_s_cmp_fn
        elif p is not None:
            term.serialize_from_rdflib(p, &t1)
            if o is not None:
                cmp_fn = cb.lookup_po_cmp_fn
                term.serialize_from_rdflib(o, &t2)
            else:
                cmp_fn = cb.lookup_p_cmp_fn
        elif o is not None:
            cmp_fn = cb.lookup_o_cmp_fn
            term.serialize_from_rdflib(o, &t1)
        else:
            cmp_fn = cb.lookup_none_cmp_fn

        # Iterate over serialized triples.
        cc.hashset_iter_init(&it, self._triples)
        while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
            trp_p = <BufferTriple*>cur
            if cmp_fn(trp_p, &t1, &t2):
                callback_fn(gr, trp_p, ctx)



cdef class Imr(Graph):
    """
    In-memory resource data container.

    This is an extension of :py:class:`~Graph` that adds a subject URI to
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
        :param args: Positional arguments inherited from
            ``Graph.__init__``.
        :param kwargs: Keyword arguments inherited from
            ``Graph.__init__``.
        """
        self.id = str(uri)
        #super().__init(*args, **kwargs)


    def __repr__(self):
        """
        String representation of an Imr.

        This includes the subject URI, number of triples contained and the
        memory address of the instance.
        """
        return (f'<{self.__class__.__name__} @{hex(id(self))} id={self.id}, '
            f'length={len(self)}>')


    def __getitem__(self, item):
        """
        Supports slicing notation.
        """
        if isinstance(item, slice):
            s, p, o = item.start, item.stop, item.step
            return self._slice(s, p, o)

        elif isinstance(item, Node):
            # If a Node is given, return all values for that predicate.
            return self._slice(self.uri, item, None)
        else:
            raise TypeError(f'Wrong slice format: {item}.')


    @property
    def uri(self):
        """
        Get resource identifier as a RDFLib URIRef.

        :rtype: rdflib.URIRef.
        """
        return URIRef(self.id)


    cdef Imr empty_copy(self):
        """
        Create an empty instance carrying over some key properties.
        """
        return self.__class__(uri=self.id)


    def value(self, p, strict=False):
        """
        Get an individual value.

        :param rdflib.termNode p: Predicate to search for.
        :param bool strict: If set to ``True`` the method raises an error if
            more than one value is found. If ``False`` (the default) only
            the first found result is returned.
        :rtype: rdflib.term.Node
        """
        # TODO use slice.
        values = {trp[2] for trp in self.lookup((self.uri, p, None))}

        if strict and len(values) > 1:
            raise RuntimeError('More than one value found for {}, {}.'.format(
                    self.id, p))

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


