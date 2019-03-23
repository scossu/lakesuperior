import logging

import rdflib

from lakesuperior import env

from cpython.object cimport Py_LT, Py_EQ, Py_GT, Py_LE, Py_NE, Py_GE
from libc.string cimport memcpy
from libc.stdlib cimport free

cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.model.callbacks as cb
cimport lakesuperior.model.structures.keyset as kset

from lakesuperior.model.base cimport Key, TripleKey
from lakesuperior.model.rdf cimport term
from lakesuperior.model.rdf.triple cimport BufferTriple
from lakesuperior.model.structures.hash cimport term_hash_seed32
from lakesuperior.model.structures.keyset cimport Keyset

logger = logging.getLogger(__name__)


cdef class Graph:
    """
    Fast and simple implementation of a graph.

    Most functions should mimic RDFLib's graph with less overhead. It uses
    the same funny but functional slicing notation.

    A Graph contains a :py:class:`lakesuperior.model.structures.keyset.Keyset`
    at its core and is bound to a
    :py:class:`~lakesuperior.store.ldp_rs.lmdb_triplestore.LmdbTriplestore`.
    This makes lookups and boolean operations very efficient because all these
    operations are performed on an array of integers.

    In order to retrieve RDF values from a ``Graph``, the underlying store
    must be looked up. This can be done in a different transaction than the
    one used to create or otherwise manipulate the graph.

    Every time a term is looked up or added to even a temporary graph, that
    term is added to the store and creates a key. This is because in the
    majority of cases that term is likely to be stored permanently anyway, and
    it's more efficient to hash it and allocate it immediately. A cleanup
    function to remove all orphaned terms (not in any triple or context index)
    can be later devised to compact the database.

    An instance of this class can also be converted to a ``rdflib.Graph``
    instance.
    """

    def __cinit__(
        self, store=None, size_t capacity=0, uri=None, set data=set()
    ):
        """
        Initialize the graph, optionally from Python/RDFlib data.

        When initializing a non-empty Graph, a store transaction must be
        opened::

            >>> from rdflib import URIRef
            >>> from lakesuperior import env_setup, env
            >>> store = env.app_globals.rdf_store
            >>> # Or alternatively:
            >>> # from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore
            >>> # store = LmdbStore('/tmp/test')
            >>> trp = {(URIRef('urn:s:0'), URIRef('urn:p:0'), URIRef('urn:o:0'))}
            >>> with store.txn_ctx():
            >>>     gr = Graph(store, data=trp)

        Similarly, any operation such as adding, changing or looking up triples
        needs a store transaction.

        Note that, even though any operation may involve adding new terms to
        the store, a read-only transaction is sufficient. Lakesuperior will
        open a write transaction automatically only if necessary and only for
        the time needed to enter the new terms.

        :type store: lakesuperior.store.ldp_rs.lmdb_triplestore.LmdbTriplestore
        :param store: Triplestore where keys are mapped to terms. By default
            this is the default application store
            (``env.app_globals.rdf_store``).

        :param size_t capacity: Initial number of allocated triples.

        :param str uri: If specified, the graph becomes a named graph and can
            utilize the :py:meth:`value()` method and special slicing notation.

        :param set data: If specified, ``capacity`` is ignored and an initial key
            set is created from a set of 3-tuples of :py:class:``rdflib.Term``
            instances.
        """
        self.uri = rdflib.URIRef(uri) if uri else None

        self.store = store if store is not None else env.app_globals.rdf_store
        logger.debug(f'Assigned store at {self.store.env_path}')

        # Initialize empty data set.
        if data:
            # Populate with provided Python set.
            self.keys = Keyset(len(data))
            self.add(data)
        else:
            self.keys = Keyset(capacity)


    ## PROPERTIES ##

    property data:
        def __get__(self):
            """
            Triple data as a Python/RDFlib set.

            :rtype: set
            """
            cdef TripleKey spok

            ret = set()

            self.keys.seek()
            while self.keys.get_next(&spok):
                ret.add((
                    self.store.from_key(spok[0]),
                    self.store.from_key(spok[1]),
                    self.store.from_key(spok[2])
                ))

            return ret


    property capacity:
        def __get__(self):
            """
            Total capacity of the underlying Keyset, in number of triples.
            """
            return self.keys.capacity


    property txn_ctx:
        def __get__(self):
            """ Expose underlying store's ``txn_ctx``. """
            return self.store.txn_ctx


    ## MAGIC METHODS ##

    def __len__(self):
        """ Number of triples in the graph. """
        return self.keys.size()


    def __richcmp__(self, other, int op):
        """ Comparators between ``Graph`` instances. """
        if op == Py_LT:
            raise NotImplementedError()
        elif op == Py_EQ:
            logger.info('Comparing for equality.')
            return len(self ^ other) == 0
        elif op == Py_GT:
            raise NotImplementedError()
        elif op == Py_LE:
            raise NotImplementedError()
        elif op == Py_NE:
            return len(self ^ other) != 0
        elif op == Py_GE:
            raise NotImplementedError()


    def __repr__(self):
        """
        String representation of the graph.

        This includes the subject URI, number of triples contained and the
        memory address of the instance.
        """
        uri_repr = f', uri={self.uri}' if self.uri else ''
        return (
            f'<{self.__class__.__module__}.{self.__class__.__qualname__} '
            f'@0x{id(self):02x} length={len(self)}{uri_repr}>'
        )


    def __str__(self):
        """ String dump of the graph triples. """
        return str(self.data)


    def __add__(self, other):
        """ Alias for set-theoretical union. """
        return self.__or__(other)


    def __iadd__(self, other):
        """ Alias for in-place set-theoretical union. """
        return self.__ior__(other)


    def __sub__(self, other):
        """ Set-theoretical subtraction. """
        cdef Graph gr3 = self.empty_copy()

        gr3.keys = kset.subtract(self.keys, other.keys)

        return gr3


    def __isub__(self, other):
        """ In-place set-theoretical subtraction. """
        self.keys = kset.subtract(self.keys, other.keys)

        return self

    def __and__(self, other):
        """ Set-theoretical intersection. """
        cdef Graph gr3 = self.empty_copy()

        gr3.keys = kset.intersect(self.keys, other.keys)

        return gr3


    def __iand__(self, other):
        """ In-place set-theoretical intersection. """
        self.keys = kset.intersect(self.keys, other.keys)

        return self


    def __or__(self, other):
        """ Set-theoretical union. """
        cdef Graph gr3 = self.empty_copy()

        gr3.keys = kset.merge(self.keys, other.keys)

        return gr3


    def __ior__(self, other):
        """ In-place set-theoretical union. """
        self.keys = kset.merge(self.keys, other.keys)

        return self


    def __xor__(self, other):
        """ Set-theoretical exclusive disjunction (XOR). """
        cdef Graph gr3 = self.empty_copy()

        gr3.keys = kset.xor(self.keys, other.keys)

        return gr3


    def __ixor__(self, other):
        """ In-place set-theoretical exclusive disjunction (XOR). """
        self.keys = kset.xor(self.keys, other.keys)

        return self


    def __contains__(self, trp):
        """
        Whether the graph contains a triple.

        :rtype: boolean
        """
        cdef TripleKey spok

        spok = [
            self.store.to_key(trp[0]),
            self.store.to_key(trp[1]),
            self.store.to_key(trp[2]),
        ]

        return self.keys.contains(&spok)


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
        elif self.uri and isinstance(item, rdflib.term.Identifier):
            # If a Node is given, return all values for that predicate.
            return self._slice(self.uri, item, None)
        else:
            raise TypeError(f'Wrong slice format: {item}.')


    def __hash__(self):
        """ TODO Not that great of a hash. """
        return id(self)


    ## BASIC PYTHON-ACCESSIBLE SET OPERATIONS ##

    def value(self, p, strict=False):
        """
        Get an individual value.

        :param rdflib.termNode p: Predicate to search for.
        :param bool strict: If set to ``True`` the method raises an error if
            more than one value is found. If ``False`` (the default) only
            the first found result is returned.
        :rtype: rdflib.term.Node
        """
        if not self.uri:
            raise ValueError('Cannot use `value` on a non-named graph.')

        # TODO use slice.
        values = {trp[2] for trp in self.lookup((self.uri, p, None))}

        if strict and len(values) > 1:
            raise RuntimeError('More than one value found for {}, {}.'.format(
                    self.uri, p))

        for ret in values:
            return ret

        return None


    def terms_by_type(self, type):
        """
        Get all terms of a type: subject, predicate or object.

        :param str type: One of ``s``, ``p`` or ``o``.
        """
        i = 'spo'.index(type)
        return {r[i] for r in self.data}


    def add(self, triples):
        """
        Add triples to the graph.

        This method checks for duplicates.

        :param iterable triples: iterable of 3-tuple triples.
        """
        cdef:
            TripleKey spok

        for s, p, o in triples:
            logger.info(f'Adding {s} {p} {o} to store: {self.store}')
            spok = [
                self.store.to_key(s),
                self.store.to_key(p),
                self.store.to_key(o),
            ]

            self.keys.add(&spok, True)


    def remove(self, pattern):
        """
        Remove triples by pattern.

        The pattern used is similar to :py:meth:`LmdbTripleStore.delete`.
        """
        # create an empty copy of the current object.
        new_gr = self.empty_copy()

        # Reverse lookup: only triples not matching the pattern are added to
        # the new set.
        self._match_ptn_callback(
            pattern, new_gr, add_trp_callback, False
        )

        # Replace the keyset.
        self.keys = new_gr.keys


    ## CYTHON-ACCESSIBLE BASIC METHODS ##

    cpdef Graph copy(self, str uri=None):
        """
        Create copy of the graph with a different (or no) URI.

        :param str uri: URI of the new graph. This should be different from
            the original.
        """
        cdef Graph new_gr = Graph(self.store, self.capacity, uri=uri)

        new_gr.keys = self.keys.copy()

        return new_gr


    cpdef Graph empty_copy(self, str uri=None):
        """
        Create an empty copy with same capacity and store binding.

        :param str uri: URI of the new graph. This should be different from
            the original.
        """
        return Graph(self.store, self.capacity, uri=uri)


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
        gr = rdflib.Graph(identifier=self.uri)
        for trp in self.data:
            gr.add(trp)

        return gr


    def _slice(self, s, p, o):
        """
        Return terms filtered by other terms.

        This behaves like the rdflib.Graph slicing policy.
        """
        logger.info(f'Slicing: {s} {p} {o}')
        # If no terms are unbound, check for containment.
        if s is not None and p is not None and o is not None: # s p o
            return (s, p, o) in self

        # If some terms are unbound, do a lookup.
        res = self.lookup((s, p, o))
        logger.info(f'Slicing results: {res}')
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
            Graph res_gr = self.empty_copy()

        self._match_ptn_callback(pattern, res_gr, add_trp_callback)
        res_gr.keys.resize()

        return res_gr


    cdef void _match_ptn_callback(
        self, pattern, Graph gr, lookup_callback_fn_t callback_fn,
        bint callback_cond=True, void* ctx=NULL
    ) except *:
        """
        Execute an arbitrary function on a list of triples matching a pattern.

        The arbitrary function is applied to each triple found in the current
        graph, and to a discrete graph that can be the current graph itself
        or a different one.

        :param tuple pattern: A 3-tuple of rdflib terms or None.
        :param Graph gr: The graph instance to apply the callback function to.
        :param lookup_callback_fn_t callback_fn: A callback function to be
            applied to the target graph using the matching triples.
        :param bint callback_cond: Whether to apply the callback function if
            a match is found (``True``) or if it is not found (``False``).
        :param void* ctx: Pointer to an arbitrary object that can be used by
            the callback function.
        """
        cdef:
            kset.key_cmp_fn_t cmp_fn
            Key k1, k2, k3
            TripleKey spok

        s, p, o = pattern

        logger.info(f'Match Callback pattern: {pattern}')

        self.keys.seek()
        # Decide comparison logic outside the loop.
        if all(pattern):
            if callback_cond:
                # Shortcut for 3-term match—only if callback_cond is True.
                spok = [
                    self.store.to_key(s),
                    self.store.to_key(p),
                    self.store.to_key(o),
                ]
                if self.keys.contains(&spok):
                    callback_fn(gr, &spok, ctx)
            else:
                # For negative condition (i.e. "apply this function to all keys
                # except the matching one"), the whole set must be scanned.
                #logger.info('All terms bound and negative condition.')
                k1 = self.store.to_key(s)
                k2 = self.store.to_key(p)
                k3 = self.store.to_key(o)
                #logger.info(f'Keys to match: {k1} {k2} {k3}')
                while self.keys.get_next(&spok):
                    #logger.info(f'Verifying spok: {spok}')
                    if k1 != spok[0] or k2 != spok[1] or k3 != spok[2]:
                        logger.info(f'Calling function for spok: {spok}')
                        callback_fn(gr, &spok, ctx)
            return

        if s is not None:
            k1 = self.store.to_key(s)
            if p is not None:
                k2 = self.store.to_key(p)
                cmp_fn = cb.lookup_skpk_cmp_fn
                logger.info('SKPK')
            elif o is not None:
                k2 = self.store.to_key(o)
                cmp_fn = cb.lookup_skok_cmp_fn
                logger.info('SKOK')
            else:
                cmp_fn = cb.lookup_sk_cmp_fn
        elif p is not None:
            k1 = self.store.to_key(p)
            if o is not None:
                k2 = self.store.to_key(o)
                cmp_fn = cb.lookup_pkok_cmp_fn
            else:
                cmp_fn = cb.lookup_pk_cmp_fn
        elif o is not None:
            k1 = self.store.to_key(o)
            cmp_fn = cb.lookup_ok_cmp_fn
        else:
            cmp_fn = cb.lookup_none_cmp_fn

        logger.info(f'k1: {k1} k2: {k2}')
        # Iterate over serialized triples.
        while self.keys.get_next(&spok):
            logger.info(f'Verifying spok: {spok}')
            if cmp_fn(&spok, k1, k2) == callback_cond:
                callback_fn(gr, &spok, ctx)



## FACTORY METHODS

def from_rdf(store=None, uri=None, *args, **kwargs):
    """
    Create a Graph from a serialized RDF string.

    This factory function takes the same arguments as
    :py:meth:`rdflib.Graph.parse`.

    :param store: see :py:meth:`Graph.__cinit__`.

    :param uri: see :py:meth:`Graph.__cinit__`.

    :param *args: Positional arguments passed to RDFlib's ``parse``.

    :param *kwargs: Keyword arguments passed to RDFlib's ``parse``.

    :rtype: Graph
    """
    gr = rdflib.Graph().parse(*args, **kwargs)

    logger.info(f'graph: {set(gr)}')
    return Graph(store=store, uri=uri, data={*gr})


## LOOKUP CALLBACK FUNCTIONS

cdef inline void add_trp_callback(
    Graph gr, const TripleKey* spok_p, void* ctx
):
    """
    Add a triple to a graph as a result of a lookup callback.
    """
    gr.keys.add(spok_p)
