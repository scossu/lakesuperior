import logging
import sys

import rdflib

#from cython.parallel import prange
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI

from lakesuperior.store.base_lmdb_store import (
        KeyExistsError, KeyNotFoundError, LmdbError)

from libc.stdlib cimport malloc, free

cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.cy_include.cylmdb as lmdb

from lakesuperior.model.base cimport (
    FIRST_KEY, KLEN, DBL_KLEN, TRP_KLEN, QUAD_KLEN,
    Key, DoubleKey, TripleKey, QuadKey,
    Buffer, buffer_dump
)
from lakesuperior.store.base_lmdb_store cimport (
        _check, BaseLmdbStore, data_v, dbi, key_v)
from lakesuperior.model.rdf.graph cimport Graph
from lakesuperior.model.rdf.term cimport (
        Term, deserialize_to_rdflib, serialize_from_rdflib)
from lakesuperior.model.rdf.triple cimport BufferTriple
from lakesuperior.model.structures.hash cimport (
        HLEN_128 as HLEN, Hash128, hash128)


# Integer keys and values are stored in the system's native byte order.
# Therefore they must be parsed left-to-right if the system is big-endian,
# and right-to-left if little-endian, in order to maintain the correct
# sorting order.
BIG_ENDIAN = sys.byteorder == 'big'
LSUP_REVERSEKEY = 0 if BIG_ENDIAN else lmdb.MDB_REVERSEKEY
LSUP_REVERSEDUP = 0 if BIG_ENDIAN else lmdb.MDB_REVERSEDUP

INT_KEY_MASK = lmdb.MDB_INTEGERKEY | LSUP_REVERSEKEY
INT_DUP_KEY_MASK = (
    lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED | lmdb.MDB_INTEGERKEY
    | LSUP_REVERSEKEY | LSUP_REVERSEDUP
)
INT_DUP_MASK = (
    lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED | lmdb.MDB_INTEGERDUP
    | LSUP_REVERSEKEY | LSUP_REVERSEDUP
)

lookup_rank = [0, 2, 1]
"""
Order in which keys are looked up if two terms are bound.
The indices with the smallest average number of values per key should be
looked up first.

0 = s:po
1 = p:so
2 = o:sp

If we want to get fancy, this can be rebalanced from time to time by
looking up the number of keys in (s:po, p:so, o:sp).
"""
lookup_ordering = [
    [0, 1, 2], # spo
    [1, 0, 2], # pso
    [2, 0, 1], # osp
]
lookup_ordering_2bound = [
    [1, 2, 0], # po:s
    [0, 2, 1], # so:p
    [0, 1, 2], # sp:o
]


logger = logging.getLogger(__name__)


cdef class LmdbTriplestore(BaseLmdbStore):
    """
    Low-level storage layer.

    This class extends the RDFLib-compatible :py:class:`BaseLmdbStore` and maps
    triples and contexts to key-value records in LMDB.

    This class uses the original LMDB C API rather than the Python bindings,
    because several data manipulations happen after retrieval from the store,
    which are more efficiently performed at the C level.
    """

    dbi_labels = [
        # Main data
        # Term key to serialized term content
        't:st',
        # Joined triple keys to context key
        'spo:c',
        # This has empty values and is used to keep track of empty contexts.
        'c:',
        # Prefix to namespace
        'pfx:ns',

        # Indices
        # Namespace to prefix
        'ns:pfx',
        # Term hash to triple key
        'th:t',
        # Lookups
        's:po',
        'p:so',
        'o:sp',
        'po:s',
        'so:p',
        'sp:o',
        'c:spo',
    ]

    lookup_indices = [
        b's:po',
        b'p:so',
        b'o:sp',
        b'po:s',
        b'so:p',
        b'sp:o',
    ]

    dbi_flags = {
        'c': INT_KEY_MASK,
        't:st': INT_KEY_MASK,
        's:po': INT_DUP_KEY_MASK,
        'p:so': INT_DUP_KEY_MASK,
        'o:sp': INT_DUP_KEY_MASK,
        'po:s': INT_DUP_MASK,
        'so:p': INT_DUP_MASK,
        'sp:o': INT_DUP_MASK,
        'c:spo': INT_DUP_KEY_MASK,
        'spo:c': INT_DUP_MASK,
    }
    logger.debug(f'DBI flags: {dbi_flags}')

    flags = 0

    options = {
        'map_size': 1024 ** 4 # 1Tb.
    }


    # DB management methods.

    cpdef dict stats(self):
        """
        Gather statistics about the database."""
        st = self._stats()
        st['num_triples'] = st['db_stats']['spo:c']['ms_entries']

        return st


    cpdef size_t _len(self, context=None) except -1:
        """
        Return the length of the dataset.

        The RDFLib interface defines `__len__` in a nonstandard way that
        causes a Cython compilation error, so this method is called by the
        `__len__` method of its Python counterpart.
        """
        cdef:
            size_t ct
            Key ck

        if context is not None:
            ck = self.to_key(context)
            key_v.mv_data = &ck
            key_v.mv_size = KLEN

            cur = self._cur_open('c:spo')
            try:
                _check(lmdb.mdb_cursor_get(
                        cur, &key_v, NULL, lmdb.MDB_SET))
                _check(lmdb.mdb_cursor_count(cur, &ct))
            except KeyNotFoundError:
                return 0
            else:
                return ct
            finally:
                #pass
                self._cur_close(cur)
        else:
            return self.stats()['num_triples']


    ## PRIVATE METHODS ##

    # Triple and graph methods.

    cpdef void add(self, triple, context=None, quoted=False) except *:
        """
        Add a triple and start indexing.

        :param tuple(rdflib.Identifier) triple: Tuple of three identifiers.
        :param context: Context identifier. ``None`` inserts in the default
            graph.
        :type context: rdflib.Identifier or None
        :param bool quoted: Not used.
        """
        cdef:
            lmdb.MDB_cursor *icur
            lmdb.MDB_val spo_v, c_v, null_v, key_v, data_v
            unsigned char i
            Hash128 thash
            QuadKey spock
            Buffer pk_t

        c = self._normalize_context(context)
        if c is None:
            c = RDFLIB_DEFAULT_GRAPH_URI

        s, p, o = triple
        icur = self._cur_open('th:t')
        try:
            for i, term_obj in enumerate((s, p, o, c)):
                serialize_from_rdflib(term_obj, &pk_t)
                hash128(&pk_t, &thash)
                try:
                    key_v.mv_data = thash
                    key_v.mv_size = HLEN
                    _check(lmdb.mdb_get(
                            self.txn, self.get_dbi('th:t'), &key_v, &data_v))
                    spock[i] = (<Key*>data_v.mv_data)[0]
                except KeyNotFoundError:
                    # If term_obj is not found, add it...
                    logger.debug('Hash {} not found. Adding to DB.'.format(
                            thash[: HLEN]))
                    spock[i] = self._append(&pk_t, dblabel=b't:st')

                    # ...and index it.
                    key_v.mv_data = thash
                    key_v.mv_size = HLEN
                    data_v.mv_data = spock + i
                    data_v.mv_size = KLEN
                    _check(
                        lmdb.mdb_cursor_put(icur, &key_v, &data_v, 0),
                        'Error setting key {}.'.format(thash))
        finally:
            self._cur_close(icur)

        spo_v.mv_data = spock # address of sk in spock
        spo_v.mv_size = TRP_KLEN # Grab 3 keys
        c_v.mv_data = spock + 3 # address of ck in spock
        c_v.mv_size = KLEN
        null_v.mv_data = b''
        null_v.mv_size = 0

        try:
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('c:'), &c_v, &null_v,
                lmdb.MDB_NOOVERWRITE))
        except KeyExistsError:
            pass
        try:
            # Add triple:context association.
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('spo:c'), &spo_v, &c_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass
        try:
            # Index context:triple association.
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('c:spo'), &c_v, &spo_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass

        self._index_triple(IDX_OP_ADD, [spock[0], spock[1], spock[2]])


    cpdef void add_graph(self, c) except *:
        """
        Add a graph (context) to the database.

        This creates an empty graph by associating the graph URI with the
        pickled `None` value. This prevents from removing the graph when all
        triples are removed.

        :param rdflib.URIRef graph: URI of the named graph to add.
        """
        cdef:
            lmdb.MDB_txn *_txn
            Buffer _sc
            Key ck

        c = self._normalize_context(c)

        ck = self.to_key(c)
        if not self._key_exists(<unsigned char*>&ck, KLEN, b'c:'):
            # Insert context term if not existing.
            if self.is_txn_rw:
                #logger.debug('Working in existing RW transaction.')
                _txn = self.txn
            else:
                #logger.debug('Opening a temporary RW transaction.')
                _check(lmdb.mdb_txn_begin(self.dbenv, NULL, 0, &_txn))
                # Open new R/W transactions.

            try:
                # Add to list of contexts.
                key_v.mv_data = &ck
                key_v.mv_size = KLEN
                data_v.mv_data = &ck # Whatever, length is zero anyways
                data_v.mv_size = 0
                _check(lmdb.mdb_put(
                    _txn, self.get_dbi(b'c:'), &key_v, &data_v, 0
                ))
                if not self.is_txn_rw:
                    _check(lmdb.mdb_txn_commit(_txn))
                    # Kick the main transaction to see the new terms.
                    lmdb.mdb_txn_reset(self.txn)
                    _check(lmdb.mdb_txn_renew(self.txn))
            except:
                if not self.is_txn_rw:
                    lmdb.mdb_txn_abort(_txn)
                raise


    cpdef void _remove(self, tuple triple_pattern, context=None) except *:
        cdef:
            lmdb.MDB_val spok_v, ck_v
            TripleKey spok_cur
            Key ck

        if context is not None:
            try:
                ck = self.to_key(context)
            except KeyNotFoundError:
                # If context is specified but not found, return to avoid
                # deleting the wrong triples.
                return

        # Get the matching pattern.
        match_set = self.triple_keys(triple_pattern, context)

        dcur = self._cur_open('spo:c')
        icur = self._cur_open('c:spo')

        try:
            spok_v.mv_size = TRP_KLEN
            # If context was specified, remove only associations with that context.
            match_set.keys.seek()
            if context is not None:
                ck_v.mv_data = &ck
                ck_v.mv_size = KLEN
                while match_set.keys.get_next(&spok_cur):
                    spok_v.mv_data = spok_cur
                    # Delete spo:c entry.
                    try:
                        _check(lmdb.mdb_cursor_get(
                                dcur, &spok_v, &ck_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(dcur, 0))

                        # Restore ck after delete.
                        ck_v.mv_data = &ck

                        # Delete c:spo entry.
                        try:
                            _check(lmdb.mdb_cursor_get(
                                    icur, &ck_v, &spok_v, lmdb.MDB_GET_BOTH))
                        except KeyNotFoundError:
                            pass
                        else:
                            _check(lmdb.mdb_cursor_del(icur, 0))

                        # Delete lookup indices, only if no other context
                        # association is present.

                        # spok_v has changed on mdb_cursor_del. Restore.
                        spok_v.mv_data = spok_cur
                        try:
                            _check(lmdb.mdb_cursor_get(
                                dcur, &spok_v, NULL, lmdb.MDB_SET))
                        except KeyNotFoundError:
                            self._index_triple(IDX_OP_REMOVE, spok_cur)

            # If no context is specified, remove all associations.
            else:
                logger.debug('Removing triples in all contexts.')
                # Loop over all SPO matching the triple pattern.
                while match_set.keys.get_next(&spok_cur):
                    spok_v.mv_data = spok_cur
                    # Loop over all context associations for this SPO.
                    try:
                        _check(lmdb.mdb_cursor_get(
                            dcur, &spok_v, &ck_v, lmdb.MDB_SET_KEY))
                    except KeyNotFoundError:
                        # Move on to the next SPO.
                        continue
                    else:
                        ck = (<Key*>ck_v.mv_data)[0]
                        while True:
                            # Delete c:spo association.
                            try:
                                _check(lmdb.mdb_cursor_get(
                                    icur, &ck_v, &spok_v, lmdb.MDB_GET_BOTH))
                            except KeyNotFoundError:
                                pass
                            else:
                                lmdb.mdb_cursor_del(icur, 0)
                                # Restore the pointer to the deleted SPO.
                                spok_v.mv_data = spok_cur
                            # Move on to next associated context.
                            try:
                                _check(lmdb.mdb_cursor_get(
                                    dcur, &spok_v, &ck_v, lmdb.MDB_NEXT_DUP))
                            except KeyNotFoundError:
                                break
                        # Then delete the spo:c association.
                        try:
                            _check(lmdb.mdb_cursor_get(
                                dcur, &spok_v, &ck_v, lmdb.MDB_SET))
                        except KeyNotFoundError:
                            pass
                        else:
                            lmdb.mdb_cursor_del(dcur, lmdb.MDB_NODUPDATA)
                            self._index_triple(IDX_OP_REMOVE, spok_cur)

        finally:
            self._cur_close(dcur)
            self._cur_close(icur)


    cdef void _index_triple(self, int op, TripleKey spok) except *:
        """
        Update index for a triple and context (add or remove).

        :param str op: one of ``IDX_OP_ADD`` or ``IDX_OP_REMOVE``.
        :param TripleKey spok: Triple key to index.
        """
        cdef:
            DoubleKey dbl_keys[3]
            size_t i = 0
            lmdb.MDB_val key_v, dbl_key_v

        dbl_keys = [
            [spok[1], spok[2]], # pok
            [spok[0], spok[2]], # sok
            [spok[0], spok[1]], # spk
        ]

        #logger.debug(f'''Indices:
        #spok: {[spok[0], spok[1], spok[2]]}
        #sk: {spok[0]}
        #pk: {spok[1]}
        #ok: {spok[2]}
        #pok: {dbl_keys[0]}
        #sok: {dbl_keys[1]}
        #spk: {dbl_keys[2]}
        #''')
        key_v.mv_size = KLEN
        dbl_key_v.mv_size = DBL_KLEN

        #logger.debug('Start indexing: {}.'.format(spok[: TRP_KLEN]))
        if op == IDX_OP_REMOVE:
            logger.debug(f'Remove {spok[0]} from indices.')
        else:
            logger.debug(f'Add {spok[0]} to indices.')

        while i < 3:
            cur1 = self._cur_open(self.lookup_indices[i]) # s:po, p:so, o:sp
            cur2 = self._cur_open(self.lookup_indices[i + 3])# po:s, so:p, sp:o
            try:
                key_v.mv_data = spok + i
                dbl_key_v.mv_data = dbl_keys[i]

                # Removal op indexing.
                if op == IDX_OP_REMOVE:
                    try:
                        _check(lmdb.mdb_cursor_get(
                                cur1, &key_v, &dbl_key_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(cur1, 0))

                    # Restore pointers after delete.
                    key_v.mv_data = spok + i
                    dbl_key_v.mv_data = dbl_keys[i]
                    try:
                        _check(lmdb.mdb_cursor_get(
                                cur2, &dbl_key_v, &key_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(cur2, 0))

                # Addition op indexing.
                elif op == IDX_OP_ADD:
                    try:
                        _check(lmdb.mdb_cursor_put(
                                cur1, &key_v, &dbl_key_v, lmdb.MDB_NODUPDATA))
                    except KeyExistsError:
                        pass

                    try:
                        _check(lmdb.mdb_cursor_put(
                                cur2, &dbl_key_v, &key_v, lmdb.MDB_NODUPDATA))
                    except KeyExistsError:
                        pass
                else:
                    raise ValueError(
                        'Index operation \'{}\' is not supported.'.format(op))
                i += 1
            finally:
                #pass
                self._cur_close(cur1)
                self._cur_close(cur2)


    cpdef void _remove_graph(self, object gr_uri) except *:
        """
        Delete a context.
        """
        cdef:
            Hash128 chash
            Key ck
            lmdb.MDB_val ck_v, chash_v
            Buffer pk_c

        # Gather information on the graph prior to deletion.
        try:
            ck = self.to_key(gr_uri)
        except KeyNotFoundError:
            return

        # Remove all triples and indices associated with the graph.
        self._remove((None, None, None), gr_uri)
        # Remove the graph if it is in triples.
        self._remove((gr_uri, None, None))
        self._remove((None, None, gr_uri))

        # Clean up all terms related to the graph.
        serialize_from_rdflib(gr_uri, &pk_c)
        hash128(&pk_c, &chash)

        ck_v.mv_size = KLEN
        chash_v.mv_size = HLEN
        try:
            ck_v.mv_data = &ck
            _check(lmdb.mdb_del(self.txn, self.get_dbi(b'c:'), &ck_v, NULL))
            ck_v.mv_data = &ck
            _check(lmdb.mdb_del(self.txn, self.get_dbi(b't:st'), &ck_v, NULL))
            chash_v.mv_data = chash
            _check(lmdb.mdb_del(self.txn, self.get_dbi(b'th:t'), &chash_v, NULL))
        except KeyNotFoundError:
            pass


    # Lookup methods.

    def contexts(self, triple=None):
        """
        Get a list of all contexts.

        :rtype: set(URIRef)
        """
        cdef:
            size_t sz, i
            Key* match

        try:
            self.all_contexts(&match, &sz, triple)
            ret = set()

            for i in range(sz):
                ret.add(self.from_key(match[i]))
        finally:
            free(match)

        return ret


    def triples(self, triple_pattern, context=None):
        """
        Generator over matching triples.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph, if available.
        :type context: rdflib.Graph or None

        :rtype: Iterator
        :return: Generator over triples and contexts in which each result has
            the following format::

                (s, p, o), generator(contexts)

        Where the contexts generator lists all context that the triple appears
        in.
        """
        cdef:
            size_t i = 0
            TripleKey it_cur
            lmdb.MDB_val key_v, data_v

        # This sounds strange, RDFLib should be passing None at this point,
        # but anyway...
        context = self._normalize_context(context)

        logger.debug(
                'Getting triples for: {}, {}'.format(triple_pattern, context))
        rset = self.triple_keys(triple_pattern, context)

        #logger.debug('Triple keys found: {}'.format(rset.data[:rset.size]))

        cur = self._cur_open('spo:c')
        try:
            key_v.mv_size = TRP_KLEN
            rset.keys.seek()
            while rset.keys.get_next(&it_cur):
                key_v.mv_data = it_cur
                # Get contexts associated with each triple.
                contexts = []
                # This shall never be MDB_NOTFOUND.
                _check(lmdb.mdb_cursor_get(cur, &key_v, &data_v, lmdb.MDB_SET))
                while True:
                    c_uri = self.from_key((<Key*>data_v.mv_data)[0])
                    contexts.append(
                        Graph(self, uri=c_uri)
                    )
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                    except KeyNotFoundError:
                        break

                yield (
                    (
                        self.from_key((<Key*>key_v.mv_data)[0]),
                        self.from_key((<Key*>key_v.mv_data)[1]),
                        self.from_key((<Key*>key_v.mv_data)[2]),
                    ),
                    tuple(contexts)
                )
        finally:
            self._cur_close(cur)


    cpdef Graph triple_keys(
        self, tuple triple_pattern, context=None, uri=None
    ):
        """
        Top-level lookup method.

        This method is used by `triples` which returns native Python tuples,
        as well as by other methods that need to iterate and filter triple
        keys without incurring in the overhead of converting them to triples.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph or URI, or None.
        :type context: rdflib.term.Identifier or None
        """
        cdef:
            size_t ct = 0, i = 0
            lmdb.MDB_cursor *icur
            lmdb.MDB_val key_v, data_v
            Key tk, ck
            TripleKey spok
            Graph flt_res, ret

        if context is not None:
            try:
                ck = self.to_key(context)
            except KeyNotFoundError:
                # Context not found.
                return Graph(self, uri=uri)

            icur = self._cur_open('c:spo')

            try:
                key_v.mv_data = &ck
                key_v.mv_size = KLEN

                # s p o c
                if all(triple_pattern):
                    for i, term in enumerate(triple_pattern):
                        try:
                            tk = self.to_key(term)
                        except KeyNotFoundError:
                            # A term key was not found.
                            return Graph(self, uri=uri)
                        spok[i] = tk
                    data_v.mv_data = spok
                    data_v.mv_size = TRP_KLEN
                    try:
                        _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        # Triple not found.
                        #logger.debug('spok / ck pair not found.')
                        return Graph(self, uri=uri)
                    ret = Graph(self, 1, uri=uri)
                    ret.keys.add(&spok)

                    return ret

                # ? ? ? c
                elif not any(triple_pattern):
                    # Get all triples from the context
                    try:
                        _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_SET))
                    except KeyNotFoundError:
                        # Triple not found.
                        return Graph(self, uri=uri)

                    _check(lmdb.mdb_cursor_count(icur, &ct))
                    ret = Graph(self, ct, uri=uri)

                    _check(lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
                    while True:
                        # Loop over page data.
                        spok_page = <TripleKey*>data_v.mv_data
                        for i in range(data_v.mv_size // TRP_KLEN):
                            ret.keys.add(spok_page + i)

                        try:
                            # Get next page.
                            _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                        except KeyNotFoundError:
                            return ret

                # Regular lookup. Filter _lookup() results by context.
                else:
                    try:
                        res = self._lookup(triple_pattern)
                    except KeyNotFoundError:
                        return Graph(self, uri=uri)

                    key_v.mv_data = &ck
                    key_v.mv_size = KLEN
                    data_v.mv_size = TRP_KLEN

                    flt_res = Graph(self, res.capacity, uri=uri)
                    res.keys.seek()
                    while res.keys.get_next(&spok):
                        data_v.mv_data = spok
                        try:
                            # Verify that the triple is associated with the
                            # context being searched.
                            _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                        except KeyNotFoundError:
                            continue
                        else:
                            flt_res.keys.add(&spok)

                    return flt_res
            finally:
                self._cur_close(icur)

        # Unfiltered lookup. No context checked.
        else:
            try:
                res = self._lookup(triple_pattern)
            except KeyNotFoundError:
                return Graph(self, uri=uri)
            return res


    cdef Graph _lookup(self, tuple triple_pattern):
        """
        Look up triples in the indices based on a triple pattern.

        :rtype: Iterator
        :return: Matching triple keys.
        """
        cdef:
            size_t ct = 0
            lmdb.MDB_stat db_stat
            lmdb.MDB_val spok_v, ck_v
            TripleKey spok
            Key sk, pk, ok, tk1, tk2, tk3

        s, p, o = triple_pattern

        try:
            if s is not None:
                sk = self.to_key(s)
            if p is not None:
                pk = self.to_key(p)
            if o is not None:
                ok = self.to_key(o)
        except KeyNotFoundError:
            return Graph(self)

        if s is not None:
            tk1 = sk
            if p is not None:
                tk2 = pk
                # s p o
                if o is not None:
                    tk3 = ok
                    spok_v.mv_data = spok
                    spok_v.mv_size = TRP_KLEN
                    try:
                        spok = [tk1, tk2, tk3]
                        _check(lmdb.mdb_get(
                            self.txn, self.get_dbi('spo:c'), &spok_v, &ck_v))
                    except KeyNotFoundError:
                        return Graph(self)

                    matches = Graph(self, 1)
                    matches.keys.add(&spok)
                    return matches

                # s p ?
                return self._lookup_2bound(0, 1, [tk1, tk2])

            if o is not None: # s ? o
                tk2 = ok
                return self._lookup_2bound(0, 2, [tk1, tk2])

            # s ? ?
            return self._lookup_1bound(0, tk1)

        if p is not None:
            tk1 = pk
            if o is not None: # ? p o
                tk2 = ok
                return self._lookup_2bound(1, 2, [tk1, tk2])

            # ? p ?
            return self._lookup_1bound(1, tk1)

        if o is not None: # ? ? o
            tk1 = ok
            return self._lookup_1bound(2, tk1)

        # ? ? ?
        # Get all triples in the database.
        dcur = self._cur_open('spo:c')

        try:
            _check(
                lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(dcur), &db_stat
                ), 'Error gathering DB stats.'
            )
            ct = db_stat.ms_entries
            ret = Graph(self, ct)
            #logger.debug(f'Triples found: {ct}')
            if ct == 0:
                return Graph(self)

            _check(lmdb.mdb_cursor_get(
                    dcur, &key_v, &data_v, lmdb.MDB_FIRST))
            while True:
                spok = <TripleKey>key_v.mv_data
                ret.keys.add(&spok)

                try:
                    _check(lmdb.mdb_cursor_get(
                        dcur, &key_v, &data_v, lmdb.MDB_NEXT_NODUP))
                except KeyNotFoundError:
                    break

            return ret
        finally:
            self._cur_close(dcur)


    cdef Graph _lookup_1bound(self, unsigned char idx, Key luk):
        """
        Lookup triples for a pattern with one bound term.

        :param str idx_name: The index to look up as one of the keys of
            ``_lookup_ordering``.
        :param rdflib.URIRef term: Bound term to search for.

        :rtype: Iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        cdef:
            unsigned int dbflags
            unsigned char term_order[3]
            size_t ct, i
            lmdb.MDB_cursor *icur
            lmdb.MDB_val key_v, data_v
            TripleKey spok

        logger.debug(f'lookup 1bound: {idx}, {luk}')

        term_order = lookup_ordering[idx]
        icur = self._cur_open(self.lookup_indices[idx])
        logging.debug(f'DB label: {self.lookup_indices[idx]}')
        logging.debug('term order: {}'.format(term_order[: 3]))

        try:
            key_v.mv_data = &luk
            key_v.mv_size = KLEN

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_count(icur, &ct))

            # Allocate memory for results.
            ret = Graph(self, ct)

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
            while True:
                lu_dset = <DoubleKey*>data_v.mv_data
                for i in range(data_v.mv_size // DBL_KLEN):
                    spok[term_order[0]] = luk
                    spok[term_order[1]] = lu_dset[i][0]
                    spok[term_order[2]] = lu_dset[i][1]

                    ret.keys.add(&spok)

                try:
                    # Get results by the page.
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                except KeyNotFoundError:
                    return ret

        finally:
            self._cur_close(icur)


    cdef Graph _lookup_2bound(
        self, unsigned char idx1, unsigned char idx2, DoubleKey tks
    ):
        """
        Look up triples for a pattern with two bound terms.

        :param str idx1: The index to look up as one of the keys of
            ``lookup_ordering_2bound``.
        :param rdflib.URIRef term1: First bound term to search for.

        :rtype: Iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        cdef:
            unsigned char luk1_offset, luk2_offset
            unsigned int dbflags
            unsigned char term_order[3] # Lookup ordering
            size_t ct, i = 0
            lmdb.MDB_cursor* icur
            Graph ret
            DoubleKey luk
            TripleKey spok

        for i in range(3):
            if (
                    idx1 in lookup_ordering_2bound[i][: 2]
                    and idx2 in lookup_ordering_2bound[i][: 2]):
                term_order = lookup_ordering_2bound[i]
                if term_order[0] == idx1:
                    luk1_offset = 0
                    luk2_offset = 1
                else:
                    luk1_offset = 1
                    luk2_offset = 0
                dblabel = self.lookup_indices[i + 3] # skip 1bound index labels
                break

            if i == 2:
                raise ValueError(
                        'Indices {} and {} not found in LU keys.'.format(
                            idx1, idx2))

        # Compose term keys in lookup key.
        luk[luk1_offset] = tks[0]
        luk[luk2_offset] = tks[1]

        icur = self._cur_open(dblabel)

        try:
            key_v.mv_data = luk
            key_v.mv_size = DBL_KLEN

            # Count duplicates for key and allocate memory for result set.
            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_count(icur, &ct))
            ret = Graph(self, ct)

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
            while True:
                lu_dset = <Key*>data_v.mv_data
                for i in range(data_v.mv_size // KLEN):
                    spok[term_order[0]] = luk[0]
                    spok[term_order[1]] = luk[1]
                    spok[term_order[2]] = lu_dset[i]

                    ret.keys.add(&spok)

                try:
                    # Get results by the page.
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                except KeyNotFoundError:
                    return ret
        finally:
            self._cur_close(icur)


    cdef void _all_term_keys(self, term_type, cc.HashSet** tkeys) except *:
        """
        Return all keys of a (``s:po``, ``p:so``, ``o:sp``) index.
        """
        cdef:
            size_t i = 0
            lmdb.MDB_stat stat
            cc.HashSetConf tkeys_conf

        idx_label = self.lookup_indices['spo'.index(term_type)]
        icur = self._cur_open(idx_label)
        try:
            _check(lmdb.mdb_stat(self.txn, lmdb.mdb_cursor_dbi(icur), &stat))

            cc.hashset_conf_init(&tkeys_conf)
            tkeys_conf.initial_capacity = 1024
            tkeys_conf.load_factor = .75
            tkeys_conf.key_length = KLEN
            tkeys_conf.key_compare = cc.CC_CMP_POINTER
            tkeys_conf.hash = cc.POINTER_HASH

            cc.hashset_new_conf(&tkeys_conf, tkeys)

            try:
                _check(lmdb.mdb_cursor_get(
                    icur, &key_v, NULL, lmdb.MDB_FIRST))
            except KeyNotFoundError:
                return

            while True:
                cc.hashset_add(tkeys[0], key_v.mv_data)

                rc = lmdb.mdb_cursor_get(
                    icur, &key_v, NULL, lmdb.MDB_NEXT_NODUP)
                try:
                    _check(rc)
                except KeyNotFoundError:
                    return
                i += 1
        finally:
            self._cur_close(icur)


    def all_terms(self, term_type):
        """
        Return all terms of a type (``s``, ``p``, or ``o``) in the store.
        """
        cdef:
            void* cur
            cc.HashSet* tkeys
            cc.HashSetIter it

        ret = set()

        try:
            self._all_term_keys(term_type, &tkeys)
            cc.hashset_iter_init(&it, tkeys)
            while cc.hashset_iter_next(&it, &cur) != cc.CC_ITER_END:
                ret.add(self.from_key((<Key*>cur)[0]))
        finally:
            if tkeys:
                free(tkeys)

        return ret


    cpdef tuple all_namespaces(self):
        """
        Return all registered namespaces.
        """
        cdef:
            size_t i = 0
            lmdb.MDB_stat stat

        ret = []
        dcur = self._cur_open('pfx:ns')
        try:
            try:
                _check(lmdb.mdb_cursor_get(
                    dcur, &key_v, &data_v, lmdb.MDB_FIRST))
            except KeyNotFoundError:
                return tuple()

            while True:
                ret.append((
                    (<unsigned char *>key_v.mv_data)[: key_v.mv_size].decode(),
                    (<unsigned char *>data_v.mv_data)[: data_v.mv_size].decode()))
                try:
                    _check(lmdb.mdb_cursor_get(
                        dcur, &key_v, &data_v, lmdb.MDB_NEXT))
                except KeyNotFoundError:
                    return tuple(ret)

                i += 1
        finally:
            self._cur_close(dcur)


    cdef void all_contexts(
        self, Key** ctx, size_t* sz, triple=None
    ) except *:
        """
        Get a list of all contexts.
        """
        cdef:
            size_t ct
            lmdb.MDB_stat stat
            lmdb.MDB_val key_v, data_v
            TripleKey spok

        cur = (
                self._cur_open('spo:c') if triple and all(triple)
                else self._cur_open('c:'))
        try:
            if triple and all(triple):
                _check(lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(cur), &stat))

                spok = [
                    self.to_key(triple[0]),
                    self.to_key(triple[1]),
                    self.to_key(triple[2]),
                ]
                key_v.mv_data = spok
                key_v.mv_size = TRP_KLEN

                try:
                    _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_SET_KEY))
                except KeyNotFoundError:
                    ctx[0] = NULL
                    return

                ctx[0] = <Key*>malloc(stat.ms_entries * KLEN)
                sz[0] = 0

                while True:
                    ctx[0][sz[0]] = (<Key*>data_v.mv_data)[0]
                    sz[0] += 1
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                    except KeyNotFoundError:
                        break
            else:
                _check(lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(cur), &stat))

                try:
                    _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_FIRST))
                except KeyNotFoundError:
                    ctx[0] = NULL
                    return

                ctx[0] = <Key*>malloc(stat.ms_entries * KLEN)
                sz[0] = 0

                while True:
                    ctx[0][sz[0]] = (<Key*>key_v.mv_data)[0]
                    sz[0] += 1
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, NULL, lmdb.MDB_NEXT))
                    except KeyNotFoundError:
                        break

        finally:
            self._cur_close(cur)


    # Key conversion methods.

    cdef inline void lookup_term(self, const Key tk, Buffer* data) except *:
        """
        look up a term by key.

        :param Key key: The key to be looked up.
        :param Buffer *data: Buffer structure containing the serialized term.
        """
        cdef:
            lmdb.MDB_val key_v, data_v

        key_v.mv_data = &tk
        key_v.mv_size = KLEN

        _check(
            lmdb.mdb_get(
                self.txn, self.get_dbi('t:st'), &key_v, &data_v
            ),
            f'Error getting data for key \'{tk}\'.'
        )
        data.addr = data_v.mv_data
        data.sz = data_v.mv_size


    cdef object from_key(self, const Key tk):
        """
        Convert a single key into one term.

        :param Key key: The key to be converted.
        """
        cdef Buffer pk_t
        #logger.info(f'From key:{tk}')

        self.lookup_term(tk, &pk_t)
        #logger.info(f'from_key buffer: {buffer_dump(&pk_t)}')

        # TODO Make Term a class and return that.
        return deserialize_to_rdflib(&pk_t)


    cdef Key to_key(self, term) except? 0:
        """
        Convert a term into a key and insert it in the term key store.

        :param rdflib.Term term: An RDFLib term (URIRef, BNode, Literal).
        :param Key key: Key that will be produced.

        :rtype: void
        """
        cdef:
            lmdb.MDB_txn *_txn
            Hash128 thash
            Buffer pk_t
            Key tk

        #logger.info(f'Serializing term: {term}')
        serialize_from_rdflib(term, &pk_t)
        hash128(&pk_t, &thash)
        key_v.mv_data = thash
        key_v.mv_size = HLEN

        try:
            #logger.debug(
            #    f'Check {buffer_dump(&pk_t)} with hash '
            #    f'{(<unsigned char*>thash)[:HLEN]} in store before adding.'
            #)
            _check(lmdb.mdb_get(
                self.txn, self.get_dbi(b'th:t'), &key_v, &data_v)
            )

            return (<Key*>data_v.mv_data)[0]

        except KeyNotFoundError:
            #logger.info(f'Adding term {term} to store.')
            # If key is not in the store, add it.
            if self.is_txn_rw:
                # Use existing R/W transaction.
                #logger.info('Working in existing RW transaction.')
                _txn = self.txn
            else:
                # Open new R/W transaction.
                #logger.info('Opening a temporary RW transaction.')
                _check(lmdb.mdb_txn_begin(self.dbenv, NULL, 0, &_txn))

            try:
                # Main entry.
                tk = self._append(&pk_t, b't:st', txn=_txn)

                # Index.
                data_v.mv_data = &tk
                data_v.mv_size = KLEN
                _check(lmdb.mdb_put(
                    _txn, self.get_dbi(b'th:t'), &key_v, &data_v, 0
                ))
                if not self.is_txn_rw:
                    _check(lmdb.mdb_txn_commit(_txn))
                    # Kick the main transaction to see the new terms.
                    lmdb.mdb_txn_reset(self.txn)
                    _check(lmdb.mdb_txn_renew(self.txn))

                return tk
            except:
                if not self.is_txn_rw:
                    lmdb.mdb_txn_abort(_txn)
                raise


    cdef Key _append(
        self, Buffer *value,
        unsigned char *dblabel=b'', lmdb.MDB_txn *txn=NULL,
        unsigned int flags=0
        ) except? 0:
        """
        Append one or more keys and values to the end of a database.

        :param lmdb.Cursor cur: The write cursor to act on.
        :param list(bytes) values: Value(s) to append.

        :rtype: Key
        :return: Key inserted.
        """
        cdef:
            lmdb.MDB_cursor *cur
            Key new_idx
            lmdb.MDB_val key_v, data_v

        if txn is NULL:
            txn = self.txn

        cur = self._cur_open(dblabel, txn=txn)

        try:
            _check(lmdb.mdb_cursor_get(cur, &key_v, NULL, lmdb.MDB_LAST))
        except KeyNotFoundError:
            new_idx = FIRST_KEY
        else:
            new_idx = (<Key*>key_v.mv_data)[0] + 1
        finally:
            self._cur_close(cur)

        key_v.mv_data = &new_idx
        logger.debug(f'New index: {new_idx}')
        logger.debug('Key data inserted: {}'.format((<unsigned char*>key_v.mv_data)[:KLEN]))
        key_v.mv_size = KLEN
        data_v.mv_data = value.addr
        data_v.mv_size = value.sz
        lmdb.mdb_put(
                txn, self.get_dbi(dblabel), &key_v, &data_v,
                flags | lmdb.MDB_APPEND)

        return new_idx


    def _normalize_context(self, context):
        """
        Normalize a context parameter to conform to the model expectations.

        :param context: Context URI or graph.
        :type context: URIRef or Graph or None
        """
        if isinstance(context, rdflib.Graph):
            if context == self or isinstance(
                context.identifier, rdflib.Variable
            ):
                context = None
            else:
                context = context.identifier
        elif isinstance(context, str):
            context = rdflib.URIRef(context)

        return context
