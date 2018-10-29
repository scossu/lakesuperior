import logging

from functools import wraps

from rdflib import Graph
from rdflib.term import Node

from lakesuperior.store.ldp_rs.lmdb_triplestore cimport (
        TRP_KLEN, LmdbTriplestore)


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


cdef class SimpleGraph:
    """
    Fast and simple implementation of a graph.

    Most functions should mimic RDFLib's graph with less overhead. It uses
        the same funny but functional slicing notation.

    An instance of this class can be converted to a ``rdflib.Graph`` instance.
    """

    cdef:
        readonly set data

    def __init__(
            self, set data=set(), tuple lookup=(), store=None):
        """
        Initialize the graph with pre-existing data or by looking up a store.

        Either ``data``, or both ``lookup`` and ``store``, can be provided.
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
        if data:
            self.data = set(data)
        else:
            if not lookup:
                self.data = set()
            else:
                if store is None:
                    raise ValueError('Store not specified for triple lookup.')
                trp_ptn = lookup[0]
                ctx = lookup[1] if len(lookup) > 1 else None
                self._data_from_lookup(store, trp_ptn, ctx)


    cdef void _data_from_lookup(
            self, LmdbTriplestore store, tuple trp_ptn, ctx=None) except *:
        """
        Look up triples in the triplestore and load them into ``data``.

        :param tuple lookup: 3-tuple of RDFlib terms or ``None``.
        :param LmdbTriplestore store: Reference to a LMDB triplestore. This
            is normally set to ``lakesuperior.env.app_globals.rdf_store``.
        """
        cdef:
            size_t i
            unsigned char spok[TRP_KLEN]

        self.data = set()
        with store.txn_ctx():
            keyset = store.triple_keys(trp_ptn, ctx)
            for i in range(keyset.ct):
                spok = keyset.data + i * TRP_KLEN
                self.data.add(store.from_trp_key(spok[: TRP_KLEN]))


    # Basic set operations.

    def add(self, dataset):
        """ Set union. """
        self.data.add(dataset)

    def remove(self, item):
        """
        Remove one item from the graph.

        :param tuple item: A 3-tuple of RDFlib terms. Only exact terms, i.e.
            wildcards are not accepted.
        """
        self.data.remove(item)

    def __len__(self):
        """ Number of triples in the graph. """
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
        self.remove_triples((trp[0], trp[1], None))
        if None in trp:
            raise ValueError(f'Invalid triple: {trp}')
        self.data.add(trp)


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


    cdef _slice(self, s, p, o):
        """
        Return terms filtered by other terms.

        This behaves like the rdflib.Graph slicing policy.
        """
        if s is None and p is None and o is None:
            return self.data
        elif s is None and p is None:
            return {(r[0], r[1]) for r in self.data if r[2] == o}
        elif s is None and o is None:
            return {(r[0], r[2]) for r in self.data if r[1] == p}
        elif p is None and o is None:
            return {(r[1], r[2]) for r in self.data if r[0] == s}
        elif s is None:
            return {r[0] for r in self.data if r[1] == p and r[2] == o}
        elif p is None:
            return {r[1] for r in self.data if r[0] == s and r[2] == o}
        elif o is None:
            return {r[2] for r in self.data if r[0] == s and r[1] == p}
        else:
            # all given
            return (s,p,o) in self.data


    cpdef lookup(self, s, p, o):
        """
        Look up triples by a pattern.
        """
        logger.debug(f'Looking up in graph: {s}, {p}, {o}.')
        if s is None and p is None and o is None:
            return self.data
        elif s is None and p is None:
            return {r for r in self.data if r[2] == o}
        elif s is None and o is None:
            return {r for r in self.data if r[1] == p}
        elif p is None and o is None:
            return {r for r in self.data if r[0] == s}
        elif s is None:
            return {r for r in self.data if r[1] == p and r[2] == o}
        elif p is None:
            return {r for r in self.data if r[0] == s and r[2] == o}
        elif o is None:
            return {r for r in self.data if r[0] == s and r[1] == p}
        else:
            # all given
            return (s,p,o) if (s, p, o) in self.data else set()


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
    cdef:
        readonly object uri

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




