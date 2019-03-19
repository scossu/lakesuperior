import logging
import sys

from cython.parallel import prange
from rdflib import Graph
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI

from lakesuperior.model.graph.graph import Imr
from lakesuperior.store.base_lmdb_store import (
        KeyExistsError, KeyNotFoundError, LmdbError)
from lakesuperior.store.base_lmdb_store cimport _check

from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.cy_include.cylmdb as lmdb

from lakesuperior.model.base cimport (
    KLEN, DBL_KLEN, TRP_KLEN, QUAD_KLEN,
    Key, DoubleKey, TripleKey, QuadKey,
    Buffer, buffer_dump
)
from lakesuperior.model.graph.graph cimport SimpleGraph, Imr
from lakesuperior.model.graph.term cimport Term
from lakesuperior.model.graph.triple cimport BufferTriple

from lakesuperior.store.base_lmdb_store cimport (
        BaseLmdbStore, data_v, dbi, key_v)
from lakesuperior.model.graph.term cimport (
        deserialize_to_rdflib, serialize_from_rdflib)
from lakesuperior.model.structures.keyset cimport Keyset
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
            ck = self._to_key_idx(context)
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

    cpdef add(self, triple, context=None, quoted=False):
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

        # TODO: figure out how the RDFLib dispatcher is inherited
        # (and if there is a use for it in a first place)
        #Store.add(self, triple, context)

        s, p, o = triple
        #logger.debug('Trying to add a triple.')
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
                    logger.info('Hash {} with key {} found. Not adding.'.format(thash[:HLEN], spock[i]))
                except KeyNotFoundError:
                    # If term_obj is not found, add it...
                    logger.info('Hash {} not found. Adding to DB.'.format(
                            thash[: HLEN]))
                    spock[i] = self._append(&pk_t, dblabel=b't:st')

                    # ...and index it.
                    logger.info('Indexing on th:t: {}: {}'.format(
                            thash[: HLEN], spock[i]))
                    key_v.mv_data = thash
                    key_v.mv_size = HLEN
                    data_v.mv_data = spock + i
                    data_v.mv_size = KLEN
                    _check(
                        lmdb.mdb_cursor_put(icur, &key_v, &data_v, 0),
                        'Error setting key {}.'.format(thash))
        finally:
            #pass
            self._cur_close(icur)
            #logger.debug('Triple add action completed.')

        spo_v.mv_data = spock # address of sk in spock
        logger.info('Inserting quad: {}'.format([spock[0], spock[1], spock[2], spock[3]]))
        spo_v.mv_size = TRP_KLEN # Grab 3 keys
        c_v.mv_data = spock + 3 # address of ck in spock
        c_v.mv_size = KLEN
        null_v.mv_data = b''
        null_v.mv_size = 0

        #logger.debug('Adding context.')
        try:
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('c:'), &c_v, &null_v,
                lmdb.MDB_NOOVERWRITE))
        except KeyExistsError:
            pass
        #logger.debug('Added c:.')
        try:
            # Add triple:context association.
            #logger.debug('Adding spo:c: {}, {}'.format((<unsigned char*>spo_v.mv_data)[:TRP_KLEN], (<unsigned char*>c_v.mv_data)[:KLEN]))
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('spo:c'), &spo_v, &c_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass
        #logger.debug('Added spo:c.')
        try:
            # Index context:triple association.
            #logger.debug('Adding c:spo: {}, {}'.format((<unsigned char*>c_v.mv_data)[:KLEN], (<unsigned char*>spo_v.mv_data)[:TRP_KLEN]))
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('c:spo'), &c_v, &spo_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass
        #logger.debug('Added c:spo.')

        #logger.debug('All main data entered. Indexing.')
        logger.info(f'indexing add: {[spock[0], spock[1], spock[2]]}')
        self._index_triple(IDX_OP_ADD, [spock[0], spock[1], spock[2]])


    cpdef add_graph(self, graph):
        """
        Add a graph to the database.

        This creates an empty graph by associating the graph URI with the
        pickled `None` value. This prevents from removing the graph when all
        triples are removed.

        This may be called by read-only operations:
        https://github.com/RDFLib/rdflib/blob/master/rdflib/graph.py#L1623
        In which case it needs to open a write transaction. This is not ideal
        but the only way to handle datasets in RDFLib.

        :param rdflib.URIRef graph: URI of the named graph to add.
        """
        cdef Buffer _sc

        if isinstance(graph, Graph):
            graph = graph.identifier

        # FIXME This is all wrong.
        serialize_from_rdflib(graph, &_sc)
        self._add_graph(&_sc)


    cdef void _add_graph(self, Buffer *pk_gr) except *:

        """
        Add a graph.

        :param pk_gr: Pickled context URIRef object.
        :type pk_gr: Buffer*
        """
        cdef:
            Hash128 chash
            Key ck
            lmdb.MDB_txn *tmp_txn

        hash128(pk_gr, &chash)
        #logger.debug('Adding a graph.')
        if not self._key_exists(chash, HLEN, b'th:t'):
            # Insert context term if not existing.
            if self.is_txn_rw:
                tmp_txn = self.txn
            else:
                _check(lmdb.mdb_txn_begin(self.dbenv, NULL, 0, &tmp_txn))
                # Open new R/W transactions.
                #logger.debug('Opening a temporary RW transaction.')

            try:
                #logger.debug('Working in existing RW transaction.')
                # Use existing R/W transaction.
                # Main entry.
                ck = self._append(pk_gr, b't:st', txn=tmp_txn)
                logger.info(f'Added new ck with key#: {ck}')
                # Index.

                key_v.mv_data = chash
                key_v.mv_size = HLEN
                data_v.mv_data = &ck
                data_v.mv_size = KLEN
                _check(lmdb.mdb_put(
                    self.txn, self.get_dbi(b'th:t'), &key_v, &data_v, 0
                ))

                # Add to list of contexts.
                key_v.mv_data = &ck
                key_v.mv_size = KLEN
                data_v.mv_data = &ck # Whatever, length is zero anyways
                data_v.mv_size = 0
                _check(lmdb.mdb_put(
                    self.txn, self.get_dbi(b'c:'), &key_v, &data_v, 0
                ))
                if not self.is_txn_rw:
                    _check(lmdb.mdb_txn_commit(tmp_txn))
            except:
                if not self.is_txn_rw:
                    lmdb.mdb_txn_abort(tmp_txn)
                raise


    cpdef void _remove(self, tuple triple_pattern, context=None) except *:
        cdef:
            lmdb.MDB_val spok_v, ck_v
            TripleKey spok_cur
            Key ck

        #logger.debug('Removing triple: {}'.format(triple_pattern))
        if context is not None:
            try:
                ck = self._to_key_idx(context)
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
            match_set.seek()
            if context is not None:
                #logger.debug('Removing triples in matching context.')
                ck_v.mv_data = &ck
                ck_v.mv_size = KLEN
                while match_set.get_next(&spok_cur):
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
                            self._index_triple(IDX_OP_REMOVE, <TripleKey>spok_cur)

            # If no context is specified, remove all associations.
            else:
                #logger.debug('Removing triples in all contexts.')
                # Loop over all SPO matching the triple pattern.
                while match_set.get_next(&spok_cur):
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
                        logger.debug(f'Removing {<TripleKey>spok_cur} from main.')
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
                            self._index_triple(IDX_OP_REMOVE, <TripleKey>spok_cur)
                            #ck_v.mv_data = ck # Unnecessary?

        finally:
            #logger.debug('Closing spo:c in _remove.')
            self._cur_close(dcur)
            #logger.debug('Closing c:spo in _remove.')
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

        logger.info(f'''Indices:
        spok: {[spok[0], spok[1], spok[2]]}
        sk: {spok[0]}
        pk: {spok[1]}
        ok: {spok[2]}
        pok: {dbl_keys[0]}
        sok: {dbl_keys[1]}
        spk: {dbl_keys[2]}
        ''')
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
                        logger.info(
                                f'Removed: {spok[i]}, {dbl_keys[i]}')
                    except KeyNotFoundError:
                        logger.info(
                                f'Not found: {spok[i]}, {dbl_keys[i]}')
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(cur1, 0))

                    # Restore pointers after delete.
                    key_v.mv_data = spok + i
                    dbl_key_v.mv_data = dbl_keys[i]
                    try:
                        _check(lmdb.mdb_cursor_get(
                                cur2, &dbl_key_v, &key_v, lmdb.MDB_GET_BOTH))
                        logger.info(f'Removed: {dbl_keys[i]}, {spok[i]}')
                    except KeyNotFoundError:
                        logger.info(f'Not found: {dbl_keys[i]}, {spok[i]}')
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(cur2, 0))

                # Addition op indexing.
                elif op == IDX_OP_ADD:
                    logger.info('Adding to index `{}`: {}, {}'.format(
                        self.lookup_indices[i],
                        (<Key*>key_v.mv_data)[0],
                        <DoubleKey>dbl_key_v.mv_data
                    ))

                    try:
                        _check(lmdb.mdb_cursor_put(
                                cur1, &key_v, &dbl_key_v, lmdb.MDB_NODUPDATA))
                    except KeyExistsError:
                        logger.info(f'Key {spok[i]} exists already.')
                        pass

                    logger.info('Adding to index `{}`: {}, {}'.format(
                        self.lookup_indices[i + 3],
                        <DoubleKey>dbl_key_v.mv_data,
                        (<Key*>key_v.mv_data)[0]
                    ))

                    try:
                        _check(lmdb.mdb_cursor_put(
                                cur2, &dbl_key_v, &key_v, lmdb.MDB_NODUPDATA))
                    except KeyExistsError:
                        logger.info(f'Double key {dbl_keys[i]} exists already.')
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

        #logger.debug('Deleting context: {}'.format(gr_uri))
        #logger.debug('Pickled context: {}'.format(serialize(gr_uri)))

        # Gather information on the graph prior to deletion.
        try:
            ck = self._to_key_idx(gr_uri)
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

        logger.info(
                'Getting triples for: {}, {}'.format(triple_pattern, context))
        rset = self.triple_keys(triple_pattern, context)

        #logger.debug('Triple keys found: {}'.format(rset.data[:rset.size]))

        cur = self._cur_open('spo:c')
        try:
            key_v.mv_size = TRP_KLEN
            rset.seek()
            while rset.get_next(&it_cur):
                logger.info(f'it_cur address: {<size_t>it_cur:02x}')
                logger.info('it_cur: {}'.format(
                    (<unsigned char*>it_cur)[:TRP_KLEN]))
                key_v.mv_data = it_cur
                logger.info(f'it_cur address after assignment: {<size_t>it_cur:02x}')
                logger.info(f'key_v address: {<size_t>key_v.mv_data:02x}')
                logger.info('mv_data: {}'.format(
                    (<unsigned char*>key_v.mv_data)[:TRP_KLEN]))
                # Get contexts associated with each triple.
                logger.info('Checking contexts for triples: {} {} {}'.format(
                    it_cur[0],
                    it_cur[1],
                    it_cur[2],
                ))
                contexts = []
                # This shall never be MDB_NOTFOUND.
                _check(lmdb.mdb_cursor_get(cur, &key_v, &data_v, lmdb.MDB_SET))
                while True:
                    c_uri = self.from_key((<Key*>data_v.mv_data)[0])
                    contexts.append(Imr(uri=c_uri, store=self))
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                    except KeyNotFoundError:
                        break

                #logger.debug('Triple keys before yield: {}: {}.'.format(
                #    (<TripleKey>key_v.mv_data)[:TRP_KLEN], tuple(contexts)))
                yield (
                    (
                        self.from_key((<Key*>key_v.mv_data)[0]),
                        self.from_key((<Key*>key_v.mv_data)[1]),
                        self.from_key((<Key*>key_v.mv_data)[2]),
                    ),
                    tuple(contexts)
                )
                #logger.debug('After yield.')
        finally:
            self._cur_close(cur)


    cpdef SimpleGraph graph_lookup(
            self, triple_pattern, context=None, uri=None, copy=False
    ):
        """
        Create a SimpleGraph or Imr instance from buffers from the store.

        The instance is only valid within the LMDB transaction that created it.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph, if available.
        :type context: rdflib.Graph or None
        :param str uri: URI for the resource. If provided, the resource
            returned will be an Imr, otherwise a SimpleGraph.

        :rtype: Iterator
        :return: Generator over triples and contexts in which each result has
            the following format::

                (s, p, o), generator(contexts)

        Where the contexts generator lists all context that the triple appears
        in.
        """
        cdef:
            Buffer buffers[3]
            BufferTriple btrp
            SimpleGraph gr
            TripleKey spok

        btrp.s = buffers
        btrp.p = buffers + 1
        btrp.o = buffers + 2

        gr = Imr(uri=uri) if uri else SimpleGraph()

        #logger.debug(
        #        'Getting triples for: {}, {}'.format(triple_pattern, context))

        match = self.triple_keys(triple_pattern, context)
        logger.info(f'Matches in graph_lookup: {match.ct}')
        #btrp = <BufferTriple*>gr.pool.alloc(match.ct, sizeof(BufferTriple))
        #buffers = <Buffer*>gr.pool.alloc(3 * match.ct, sizeof(Buffer))

        match.seek()
        while match.get_next(&spok):
            self.lookup_term(spok, buffers)
            self.lookup_term(spok + 1, buffers + 1)
            self.lookup_term(spok + 2, buffers + 2)
            #logger.info(f'Found triple: {buffer_dump(btrp.s)} {buffer_dump(btrp.p)} {buffer_dump(btrp.o)}')

            gr.add_triple(&btrp, True)

        return gr


    cdef Keyset triple_keys(self, tuple triple_pattern, context=None):
        """
        Top-level lookup method.

        This method is used by `triples` which returns native Python tuples,
        as well as by other methods that need to iterate and filter triple
        keys without incurring in the overhead of converting them to triples.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph or URI, or None.
        :type context: rdflib.term.Identifier or None
        """
        # TODO: Improve performance by allowing passing contexts as a tuple.
        cdef:
            size_t ct = 0, i = 0
            lmdb.MDB_cursor *icur
            lmdb.MDB_val key_v, data_v
            Key tk, ck
            TripleKey spok
            Keyset flt_res, ret

        if context is not None:
            try:
                ck = self._to_key_idx(context)
            except KeyNotFoundError:
                # Context not found.
                return Keyset()

            icur = self._cur_open('c:spo')

            try:
                key_v.mv_data = &ck
                key_v.mv_size = KLEN

                # s p o c
                if all(triple_pattern):
                    #logger.debug('Lookup: s p o c')
                    for i, term in enumerate(triple_pattern):
                        try:
                            tk = self._to_key_idx(term)
                        except KeyNotFoundError:
                            # A term key was not found.
                            return Keyset()
                        spok[i] = tk
                    data_v.mv_data = spok
                    data_v.mv_size = TRP_KLEN
                    try:
                        _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        # Triple not found.
                        #logger.debug('spok / ck pair not found.')
                        return Keyset()
                    ret = Keyset(1)
                    ret.add(&spok)

                    return ret

                # ? ? ? c
                elif not any(triple_pattern):
                    # Get all triples from the context
                    #logger.debug('Lookup: ? ? ? c')
                    try:
                        _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_SET))
                    except KeyNotFoundError:
                        # Triple not found.
                        return Keyset()

                    _check(lmdb.mdb_cursor_count(icur, &ct))
                    ret = Keyset(ct)
                    #logger.debug(f'Entries in c:spo: {ct}')
                    #logger.debug(f'Allocated {ret.size} bytes.')

                    #logger.debug('Looking in key: {}'.format(
                    #    (<unsigned char *>key_v.mv_data)[:key_v.mv_size]))
                    _check(lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
                    while True:
                        #logger.debug('Data page: {}'.format(
                        #        (<unsigned char *>data_v.mv_data)[: data_v.mv_size]))
                        # Loop over page data.
                        spok_page = <TripleKey*>data_v.mv_data
                        for i in range(data_v.mv_size // TRP_KLEN):
                            ret.add(spok_page + i)

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
                        return Keyset()

                    #logger.debug('Allocating for context filtering.')
                    key_v.mv_data = &ck
                    key_v.mv_size = KLEN
                    data_v.mv_size = TRP_KLEN

                    flt_res = Keyset(res.ct)
                    res.seek()
                    while res.get_next(&spok):
                        data_v.mv_data = spok
                        #logger.debug('Checking row #{}'.format(flt_j))
                        #logger.debug('Checking c:spo {}, {}'.format(
                        #    (<unsigned char *>key_v.mv_data)[: key_v.mv_size],
                        #    (<unsigned char *>data_v.mv_data)[: data_v.mv_size]))
                        try:
                            # Verify that the triple is associated with the
                            # context being searched.
                            _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                        except KeyNotFoundError:
                            continue
                        else:
                            flt_res.add(&spok)

                    return flt_res
            finally:
                self._cur_close(icur)

        # Unfiltered lookup. No context checked.
        else:
            #logger.debug('No context in query.')
            try:
                res = self._lookup(triple_pattern)
            except KeyNotFoundError:
                return Keyset()
            #logger.debug('Res data before triple_keys return: {}'.format(
            #    res.data[: res.size]))
            return res


    cdef Keyset _lookup(self, tuple triple_pattern):
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
                sk = self._to_key_idx(s)
            if p is not None:
                pk = self._to_key_idx(p)
            if o is not None:
                ok = self._to_key_idx(o)
        except KeyNotFoundError:
            return Keyset()

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
                        return Keyset()

                    matches = Keyset(1)
                    matches.add(&spok)
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
        #logger.debug('Getting all DB triples.')
        dcur = self._cur_open('spo:c')

        try:
            _check(
                lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(dcur), &db_stat
                ), 'Error gathering DB stats.'
            )
            ct = db_stat.ms_entries
            ret = Keyset(ct)
            #logger.debug(f'Triples found: {ct}')
            if ct == 0:
                return Keyset()

            _check(lmdb.mdb_cursor_get(
                    dcur, &key_v, &data_v, lmdb.MDB_FIRST))
            while True:
                spok = <TripleKey>key_v.mv_data
                ret.add(&spok)

                try:
                    _check(lmdb.mdb_cursor_get(
                        dcur, &key_v, &data_v, lmdb.MDB_NEXT_NODUP))
                except KeyNotFoundError:
                    break

            return ret
        finally:
            self._cur_close(dcur)


    cdef Keyset _lookup_1bound(self, unsigned char idx, Key luk):
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
            #DoubleKey* lu_dset
            TripleKey spok

        logger.info(f'lookup 1bound: {idx}, {luk}')

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
            ret = Keyset(ct)
            #logger.debug(f'Entries for {self.lookup_indices[idx]}: {ct}')
            #logger.debug(f'Allocated {ret.size} bytes of data.')
            #logger.debug('First row: {}'.format(
            #        (<unsigned char *>data_v.mv_data)[:DBL_KLEN]))

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
            while True:
                lu_dset = <DoubleKey*>data_v.mv_data
                for i in range(data_v.mv_size // DBL_KLEN):
                    logger.info('Got 2-terms in lookup_1bound: {} {}'.format(
                        lu_dset[i][0], lu_dset[i][1]))
                    spok[term_order[0]] = luk
                    spok[term_order[1]] = lu_dset[i][0]
                    spok[term_order[2]] = lu_dset[i][1]
                    logger.info('Assembled triple in lookup_1bound: {} {} {}'.format(
                        spok[0], spok[1], spok[2]))

                    ret.add(&spok)
                    logger.info(f'ret count: {ret.ct}')

                try:
                    # Get results by the page.
                    logger.info('Retrieving one more page of results.')
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                except KeyNotFoundError:
                    logger.info('1bound: No more results.')
                    return ret

            #logger.debug('Assembled data in 1bound ({}): {}'.format(ret.size, ret.data[: ret.size]))
        finally:
            self._cur_close(icur)


    cdef Keyset _lookup_2bound(
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
            Keyset ret
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
        logger.info('Term order: {}'.format(term_order[:3]))
        logger.info('LUK offsets: {}, {}'.format(luk1_offset, luk2_offset))
        luk[luk1_offset] = tks[0]
        luk[luk2_offset] = tks[1]
        logger.info(f'luk: {luk[0]} {luk[1]}')

        icur = self._cur_open(dblabel)
        logger.debug('Database label: {}'.format(dblabel))

        try:
            key_v.mv_data = luk
            key_v.mv_size = DBL_KLEN

            # Count duplicates for key and allocate memory for result set.
            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_count(icur, &ct))
            ret = Keyset(ct)
            #logger.debug('Entries for {}: {}'.format(self.lookup_indices[idx], ct))
            #logger.debug('First row: {}'.format(
            #        (<unsigned char *>data_v.mv_data)[:DBL_KLEN]))

            #logger.debug('term_order: {}'.format(asm_rng[:3]))
            #logger.debug('luk: {}'.format(luk))

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
            while True:
                lu_dset = <Key*>data_v.mv_data
                for i in range(data_v.mv_size // KLEN):
                    logger.info(f'Got term in lookup_2bound: {lu_dset[i]}')
                    spok[term_order[0]] = luk[0]
                    spok[term_order[1]] = luk[1]
                    spok[term_order[2]] = lu_dset[i]

                    ret.add(&spok)

                try:
                    # Get results by the page.
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                except KeyNotFoundError:
                    logger.info('2bound: No more results.')
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
        #logger.debug('Looking for all terms in index: {}'.format(idx_label))
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
            #pass
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
            while cc.hashset_iter_next(&it, &cur):
                #logger.debug('Yielding: {}'.format(key))
                ret.add(self.from_key(<Key>cur))
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
                #logger.debug('Found namespace: {}:{}'.format(<unsigned char *>key_v.mv_data, <unsigned char *>data_v.mv_data))
                try:
                    _check(lmdb.mdb_cursor_get(
                        dcur, &key_v, &data_v, lmdb.MDB_NEXT))
                except KeyNotFoundError:
                    return tuple(ret)

                i += 1
        finally:
            #pass
            self._cur_close(dcur)


    cdef void all_contexts(
        self, Key** ctx, size_t* sz, triple=None
    ) except *:
        """
        Get a list of all contexts.

        :rtype: Iterator(lakesuperior.model.graph.graph.Imr)
        """
        cdef:
            size_t ct
            lmdb.MDB_cursor_op seek_op, scan_op
            lmdb.MDB_stat stat
            lmdb.MDB_val key_v
            TripleKey spok

        cur = (
                self._cur_open('spo:c') if triple and all(triple)
                else self._cur_open('c:'))

        key_v.mv_data = &spok
        if triple and all(triple):
            lmdb_seek_op = lmdb.MDB_SET_KEY
            lmdb_scan_op = lmdb.MDB_NEXT_DUP
            spok = [
                self._to_key_idx(triple[0]),
                self._to_key_idx(triple[1]),
                self._to_key_idx(triple[2]),
            ]
            key_v.mv_size = TRP_KLEN
        else:
            lmdb_seek_op = lmdb.MDB_FIRST
            lmdb_scan_op = lmdb.MDB_NEXT
            key_v.mv_size = 0

        try:
            _check(lmdb.mdb_stat(
                self.txn, lmdb.mdb_cursor_dbi(cur), &stat))

            try:
                _check(lmdb.mdb_cursor_get(
                        cur, &key_v, &data_v, seek_op))
            except KeyNotFoundError:
                ctx[0] = NULL
                return

            ctx[0] = <Key*>malloc(stat.ms_entries * KLEN)
            sz[0] = 0

            while True:
                ctx[0][sz[0]] = (<Key*>data_v.mv_data)[0]
                try:
                    _check(lmdb.mdb_cursor_get(
                        cur, &key_v, &data_v, scan_op))
                except KeyNotFoundError:
                    break

                sz[0] += 1

        finally:
            self._cur_close(cur)


    # Key conversion methods.

    cdef inline void lookup_term(self, const Key* tk, Buffer* data) except *:
        """
        look up a term by key.

        :param Key key: The key to be looked up.
        :param Buffer *data: Buffer structure containing the serialized term.
        """
        cdef:
            lmdb.MDB_val key_v, data_v

        key_v.mv_data = tk
        key_v.mv_size = KLEN

        _check(
            lmdb.mdb_get(
                self.txn, self.get_dbi('t:st'), &key_v, &data_v
            ),
            f'Error getting data for key \'{tk[0]}\'.'
        )
        data.addr = data_v.mv_data
        data.sz = data_v.mv_size


    cdef object from_key(self, const Key tk):
        """
        Convert a single key into one term.

        :param Key key: The key to be converted.
        """
        cdef Buffer pk_t

        logger.info(f'Looking up key: {tk}')
        self.lookup_term(&tk, &pk_t)
        logger.info(f'Serialized term found: {buffer_dump(&pk_t)}')

        # TODO Make Term a class and return that.
        return deserialize_to_rdflib(&pk_t)


    cdef inline Key _to_key_idx(self, term) except -1:
        """
        Convert a triple, quad or term into a key index (bare number).

        The key is the checksum of the serialized object, therefore unique for
        that object.

        :param rdflib.Term term: An RDFLib term (URIRef, BNode, Literal).
        :param Key key: Pointer to the key that will be produced.

        :rtype: void
        """
        cdef:
            Hash128 thash
            Buffer pk_t

        logger.info(f'Serializing term: {term}')
        serialize_from_rdflib(term, &pk_t)
        hash128(&pk_t, &thash)
        key_v.mv_data = thash
        key_v.mv_size = HLEN

        dbi = self.get_dbi('th:t')
        #logger.debug(f'DBI: {dbi}')
        _check(lmdb.mdb_get(self.txn, dbi, &key_v, &data_v))

        return (<Key*>data_v.mv_data)[0]


    cdef Key _append(
        self, Buffer *value,
        unsigned char *dblabel=b'', lmdb.MDB_txn *txn=NULL,
        unsigned int flags=0
    ):
        """
        Append one or more keys and values to the end of a database.

        :param lmdb.Cursor cur: The write cursor to act on.
        :param list(bytes) values: Value(s) to append.

        :rtype: Key
        :return: Index of key inserted.
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
            logger.debug('First key inserted here.')
            new_idx = 0
        else:
            new_idx = (<Key*>key_v.mv_data)[0] + 1
            logger.debug(f'New index value: {new_idx}')
        finally:
            #pass
            self._cur_close(cur)

        key_v.mv_data = &new_idx
        logger.debug(f'New index: {new_idx}')
        logger.debug('Key data inserted: {}'.format((<unsigned char*>key_v.mv_data)[:KLEN]))
        key_v.mv_size = KLEN
        data_v.mv_data = value.addr
        data_v.mv_size = value.sz
        logger.info('Appending value {} to db {} with key: {}'.format(
            buffer_dump(value), dblabel.decode(), new_idx))
        #logger.debug('data size: {}'.format(data_v.mv_size))
        lmdb.mdb_put(
                txn, self.get_dbi(dblabel), &key_v, &data_v,
                flags | lmdb.MDB_APPEND)

        return new_idx
