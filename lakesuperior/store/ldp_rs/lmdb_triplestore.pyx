import logging

from rdflib import Graph
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI

from lakesuperior.model.graph.graph import Imr
from lakesuperior.store.base_lmdb_store import (
        KeyExistsError, KeyNotFoundError, LmdbError)
from lakesuperior.store.base_lmdb_store cimport _check

from cython.parallel import prange
from libc.stdlib cimport free
from libc.string cimport memcpy

cimport lakesuperior.cy_include.cylmdb as lmdb
from lakesuperior.model.base cimport buffer_dump
from lakesuperior.model.graph.graph cimport SimpleGraph, Imr
from lakesuperior.model.graph.term cimport Term
from lakesuperior.model.graph.triple cimport BufferTriple

from lakesuperior.store.base_lmdb_store cimport (
        BaseLmdbStore, data_v, dbi, key_v)
from lakesuperior.model.base cimport Buffer
from lakesuperior.model.graph.term cimport (
        deserialize_to_rdflib, serialize_from_rdflib)
from lakesuperior.model.structures.keyset cimport Keyset
from lakesuperior.model.structures.hash cimport (
        HLEN_128 as HLEN, Hash128, hash128)


FIRST_KEY = <bytes>KEY_START * KLEN
"""First key of a sequence."""

IDX_OP_ADD = '_idx_add'
IDX_OP_REMOVE = '_idx_remove'

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
        's:po': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'p:so': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'o:sp': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'po:s': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'so:p': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'sp:o': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'c:spo': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'spo:c': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
    }

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

        if context is not None:
            self._to_key(context, <Key *>key_v.mv_data)
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
            lmdb.MDB_val spo_v, c_v, null_v
            unsigned char i
            Hash128 thash
            # Using Key or TripleKey here breaks Cython. This might be a bug.
            # See https://github.com/cython/cython/issues/2517
            unsigned char spock[QUAD_KLEN]
            unsigned char nkey[KLEN]
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
                    memcpy(spock + (i * KLEN), data_v.mv_data, KLEN)
                    #logger.debug('Hash {} found. Not adding.'.format(thash[: HLEN]))
                except KeyNotFoundError:
                    # If term_obj is not found, add it...
                    #logger.debug('Hash {} not found. Adding to DB.'.format(
                    #        thash[: HLEN]))
                    self._append(&pk_t, &nkey, dblabel=b't:st')
                    memcpy(spock + (i * KLEN), nkey, KLEN)

                    # ...and index it.
                    #logger.debug('Indexing on th:t: {}: {}'.format(
                    #        thash[: HLEN], nkey[: KLEN]))
                    key_v.mv_data = thash
                    key_v.mv_size = HLEN
                    data_v.mv_data = nkey
                    data_v.mv_size = KLEN
                    _check(
                        lmdb.mdb_cursor_put(icur, &key_v, &data_v, 0),
                        'Error setting key {}.'.format(thash))
        finally:
            #pass
            self._cur_close(icur)
            #logger.debug('Triple add action completed.')

        spo_v.mv_data = spock
        spo_v.mv_size = TRP_KLEN
        c_v.mv_data = spock + TRP_KLEN
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
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('spo:c'), &spo_v, &c_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass
        #logger.debug('Added spo:c.')
        try:
            # Index context:triple association.
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('c:spo'), &c_v, &spo_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass
        #logger.debug('Added c:spo.')

        #logger.debug('All main data entered. Indexing.')
        self._index_triple(IDX_OP_ADD, spock[: TRP_KLEN])


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
            unsigned char ck[KLEN]
            lmdb.MDB_txn *tmp_txn
            lmdb.MDB_cursor *th_cur
            lmdb.MDB_cursor *pk_cur
            lmdb.MDB_cursor *ck_cur

        hash128(pk_gr, &chash)
        #logger.debug('Adding a graph.')
        if not self._key_exists(chash, HLEN, b'th:t'):
            # Insert context term if not existing.
            if self.is_txn_rw:
                #logger.debug('Working in existing RW transaction.')
                # Use existing R/W transaction.
                # Main entry.
                self._append(pk_gr, &ck, b't:st')
                # Index.
                self._put(chash, HLEN, ck, KLEN, b'th:t')
                # Add to list of contexts.
                self._put(ck, KLEN, b'', 0, 'c:')
            else:
                # Open new R/W transactions.
                #logger.debug('Opening a temporary RW transaction.')
                _check(lmdb.mdb_txn_begin(self.dbenv, NULL, 0, &tmp_txn))
                try:
                    self._append(pk_gr, &ck, b't:st', txn=tmp_txn)
                    # Index.
                    self._put(chash, HLEN, ck, KLEN, b'th:t', txn=tmp_txn)
                    # Add to list of contexts.
                    self._put(ck, KLEN, b'', 0, b'c:', txn=tmp_txn)
                    _check(lmdb.mdb_txn_commit(tmp_txn))
                    #logger.debug('Temp RW transaction closed.')
                except:
                    lmdb.mdb_txn_abort(tmp_txn)
                    raise


    cpdef void _remove(self, tuple triple_pattern, context=None) except *:
        cdef:
            unsigned char spok[TRP_KLEN]
            size_t i = 0
            Key ck
            lmdb.MDB_val spok_v, ck_v

        #logger.debug('Removing triple: {}'.format(triple_pattern))
        if context is not None:
            try:
                self._to_key(context, &ck)
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
            if context is not None:
                #logger.debug('Removing triples in matching context.')
                ck_v.mv_data = ck
                ck_v.mv_size = KLEN
                while i < match_set.ct:
                    memcpy(
                            spok, match_set.data + match_set.itemsize * i,
                            TRP_KLEN)
                    spok_v.mv_data = spok
                    # Delete spo:c entry.
                    try:
                        _check(lmdb.mdb_cursor_get(
                                dcur, &spok_v, &ck_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(dcur, 0))

                        # Restore ck after delete.
                        ck_v.mv_data = ck

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
                        spok_v.mv_data = spok
                        try:
                            _check(lmdb.mdb_cursor_get(
                                dcur, &spok_v, NULL, lmdb.MDB_SET))
                        except KeyNotFoundError:
                            self._index_triple(IDX_OP_REMOVE, spok)
                    i += 1

            # If no context is specified, remove all associations.
            else:
                #logger.debug('Removing triples in all contexts.')
                # Loop over all SPO matching the triple pattern.
                while i < match_set.ct:
                    spok = match_set.data + match_set.itemsize * i
                    spok_v.mv_data = spok
                    # Loop over all context associations for this SPO.
                    try:
                        _check(lmdb.mdb_cursor_get(
                            dcur, &spok_v, &ck_v, lmdb.MDB_SET_KEY))
                    except KeyNotFoundError:
                        # Move on to the next SPO.
                        continue
                    else:
                        ck = <Key>ck_v.mv_data
                        logger.debug(f'Removing {spok[: TRP_KLEN]} from main.')
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
                                spok_v.mv_data = spok
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
                            self._index_triple(IDX_OP_REMOVE, spok)
                            #ck_v.mv_data = ck # Unnecessary?
                    finally:
                        i += 1

        finally:
            #pass
            #logger.debug('Closing spo:c in _remove.')
            self._cur_close(dcur)
            #logger.debug('Closing c:spo in _remove.')
            self._cur_close(icur)


    cdef void _index_triple(self, str op, TripleKey spok) except *:
        """
        Update index for a triple and context (add or remove).

        :param str op: 'add' or 'remove'.
        :param TripleKey spok: Triple key.
        """
        cdef:
            unsigned char keys[3][KLEN]
            unsigned char dbl_keys[3][DBL_KLEN]
            size_t i = 0
            lmdb.MDB_val key_v, dbl_key_v

        keys[0] = spok # sk
        keys[1] = spok + KLEN # pk
        keys[2] = spok + DBL_KLEN # ok

        dbl_keys[0] = spok + KLEN # pok
        memcpy(&dbl_keys[1], spok, KLEN) # sok, 1st part
        memcpy(&dbl_keys[1][KLEN], spok + DBL_KLEN, KLEN) # sok, 2nd part
        dbl_keys[2] = spok # spk
        #logger.debug('''Indices:
        #spok: {}
        #sk: {}
        #pk: {}
        #ok: {}
        #pok: {}
        #sok: {}
        #spk: {}
        #'''.format(
        #        spok[:TRP_KLEN],
        #        keys[0][:KLEN], keys[1][:KLEN], keys[2][:KLEN],
        #        dbl_keys[0][:DBL_KLEN], dbl_keys[1][:DBL_KLEN], dbl_keys[2][:DBL_KLEN]))
        key_v.mv_size = KLEN
        dbl_key_v.mv_size = DBL_KLEN

        #logger.debug('Start indexing: {}.'.format(spok[: TRP_KLEN]))
        if op == IDX_OP_REMOVE:
            logger.debug(f'Remove {spok[ : TRP_KLEN]} from indices.')
        else:
            logger.debug(f'Add {spok[ : TRP_KLEN]} to indices.')

        while i < 3:
            cur1 = self._cur_open(self.lookup_indices[i]) # s:po, p:so, o:sp
            cur2 = self._cur_open(self.lookup_indices[i + 3])# po:s, so:p, sp:o
            try:
                key_v.mv_data = keys[i]
                dbl_key_v.mv_data = dbl_keys[i]

                # Removal op indexing.
                if op == IDX_OP_REMOVE:
                    try:
                        _check(lmdb.mdb_cursor_get(
                                cur1, &key_v, &dbl_key_v, lmdb.MDB_GET_BOTH))
                        logger.debug(f'Removed: {keys[i][: KLEN]}, '
                                f'{dbl_keys[i][: DBL_KLEN]}')
                    except KeyNotFoundError:
                        logger.debug(f'Not found in index: {keys[i][: KLEN]}, '
                                f'{dbl_keys[i][: DBL_KLEN]}')
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(cur1, 0))

                    # Restore pointers after delete.
                    key_v.mv_data = keys[i]
                    dbl_key_v.mv_data = dbl_keys[i]
                    try:
                        _check(lmdb.mdb_cursor_get(
                                cur2, &dbl_key_v, &key_v, lmdb.MDB_GET_BOTH))
                        logger.debug(f'Removed: {dbl_keys[i][: DBL_KLEN]}, '
                                f'{keys[i][: KLEN]}')
                    except KeyNotFoundError:
                        logger.debug(f'Not found in index: '
                                f'{dbl_keys[i][: DBL_KLEN]}, '
                                f'{keys[i][: KLEN]}')
                        pass
                    else:
                        _check(lmdb.mdb_cursor_del(cur2, 0))

                # Addition op indexing.
                elif op == IDX_OP_ADD:
                    logger.debug('Adding to index `{}`: {}, {}'.format(
                        self.lookup_indices[i],
                        (<unsigned char *>key_v.mv_data)[ : key_v.mv_size],
                        (<unsigned char *>dbl_key_v.mv_data)[ : dbl_key_v.mv_size]))

                    try:
                        _check(lmdb.mdb_cursor_put(
                                cur1, &key_v, &dbl_key_v, lmdb.MDB_NODUPDATA))
                    except KeyExistsError:
                        logger.debug(f'Key {keys[i][: KLEN]} exists already.')
                        pass

                    logger.debug('Adding to index `{}`: {}, {}'.format(
                        self.lookup_indices[i + 3],
                        (<unsigned char *>dbl_key_v.mv_data)[ : dbl_key_v.mv_size],
                        (<unsigned char *>key_v.mv_data)[ : key_v.mv_size]))

                    try:
                        _check(lmdb.mdb_cursor_put(
                                cur2, &dbl_key_v, &key_v, lmdb.MDB_NODUPDATA))
                    except KeyExistsError:
                        logger.debug(f'Double key {dbl_keys[i][: DBL_KLEN]} exists already.')
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
            unsigned char ck[KLEN]
            lmdb.MDB_val ck_v, chash_v
            Buffer pk_c

        #logger.debug('Deleting context: {}'.format(gr_uri))
        #logger.debug('Pickled context: {}'.format(serialize(gr_uri)))

        # Gather information on the graph prior to deletion.
        try:
            self._to_key(gr_uri, &ck)
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
            ck_v.mv_data = ck
            _check(lmdb.mdb_del(self.txn, self.get_dbi(b'c:'), &ck_v, NULL))
            ck_v.mv_data = ck
            _check(lmdb.mdb_del(self.txn, self.get_dbi(b't:st'), &ck_v, NULL))
            chash_v.mv_data = chash
            _check(lmdb.mdb_del(self.txn, self.get_dbi(b'th:t'), &chash_v, NULL))
        except KeyNotFoundError:
            pass


    # Lookup methods.

    def contexts(self, triple=None):
        """
        Get a list of all contexts.

        :rtype: Iterator(lakesuperior.model.graph.graph.Imr)
        """
        for ctx_uri in self.all_contexts(triple):
            yield Imr(uri=self.from_key(ctx_uri), store=self)


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
            size_t i = 0, j = 0
            unsigned char spok[TRP_KLEN]
            unsigned char ck[KLEN]
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
            for i in range(rset.ct):
                #logger.debug('Checking contexts for triples: {}'.format(
                #    (rset.data + i * TRP_KLEN)[:TRP_KLEN]))
                key_v.mv_data = rset.data + i * TRP_KLEN
                # Get contexts associated with each triple.
                contexts = []
                # This shall never be MDB_NOTFOUND.
                _check(lmdb.mdb_cursor_get(cur, &key_v, &data_v, lmdb.MDB_SET))
                while True:
                    c_uri = self.from_key(<Key>data_v.mv_data)
                    contexts.append(Imr(uri=c_uri, store=self))
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                    except KeyNotFoundError:
                        break

                #logger.debug('Triple keys before yield: {}: {}.'.format(
                #    (<TripleKey>key_v.mv_data)[:TRP_KLEN], tuple(contexts)))
                yield self.from_trp_key(
                    (<TripleKey>key_v.mv_data)[: TRP_KLEN]), tuple(contexts)
                #logger.debug('After yield.')
        finally:
            self._cur_close(cur)


    cpdef SimpleGraph graph_lookup(self, triple_pattern, context=None):
        """
        Create a SimpleGraph instance from "borrowed" buffers from the store.

        The instance is only valid within the LMDB transaction that created it.

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
            unsigned char* spok
            size_t cur = 0
            SimpleGraph gr = SimpleGraph()

        logger.debug(
                'Getting triples for: {}, {}'.format(triple_pattern, context))

        spok_a = self.triple_keys(triple_pattern, context)
        btrp = <BufferTriple*>gr.pool.alloc(spok_a.ct, sizeof(BufferTriple))
        buffers = <Buffer*>gr.pool.alloc(3 * spok_a.ct, sizeof(Buffer))

        spok_a.iter_init()
        while spok_a.iter_next(&spok):
            btrp[cur].s = buffers + cur * 3
            btrp[cur].p = buffers + cur * 3 + 1
            btrp[cur].o = buffers + cur * 3 + 2

            #logger.info('Looking up key: {}'.format(spok[:KLEN]))
            self.lookup_term(spok, buffers + cur * 3)
            #logger.info(f'Found triple s: {buffer_dump(btrp[cur].s)}')
            #logger.info('Looking up key: {}'.format(spok[KLEN:DBL_KLEN]))
            self.lookup_term(spok + KLEN, buffers + cur * 3 + 1)
            #logger.info(f'Found triple p: {buffer_dump(btrp[cur].p)}')
            #logger.info('Looking up key: {}'.format(spok[DBL_KLEN:TRP_KLEN]))
            self.lookup_term(spok + DBL_KLEN, buffers + cur * 3 + 2)
            #logger.info(f'Found triple o: {buffer_dump(btrp[cur].o)}')

            gr.add_triple(btrp + cur)
            cur += 1

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
            unsigned char tk[KLEN]
            unsigned char ck[KLEN]
            unsigned char spok[TRP_KLEN]
            #unsigned char *pk_c
            size_t ct = 0, flt_j = 0, i = 0, j = 0, pg_offset = 0, c_size
            lmdb.MDB_cursor *icur
            lmdb.MDB_val key_v, data_v
            Keyset flt_res, ret

        if context is not None:
            #serialize(context, &pk_c, &c_size)
            try:
                self._to_key(context, &ck)
            except KeyNotFoundError:
                # Context not found.
                return Keyset(0, TRP_KLEN)

            icur = self._cur_open('c:spo')

            try:
                key_v.mv_data = ck
                key_v.mv_size = KLEN

                # s p o c
                if all(triple_pattern):
                    #logger.debug('Lookup: s p o c')
                    for i, term in enumerate(triple_pattern):
                        try:
                            self._to_key(term, &tk)
                        except KeyNotFoundError:
                            # Context not found.
                            return Keyset(0, TRP_KLEN)
                        memcpy(spok + (KLEN * i), tk, KLEN)
                        if tk is NULL:
                            # A term in the triple is not found.
                            return Keyset(0, TRP_KLEN)
                    data_v.mv_data = spok
                    data_v.mv_size = TRP_KLEN
                    #logger.debug(
                    #        'Found spok {}. Matching with context {}'.format(
                    #            (<TripleKey>data_v.mv_data)[: TRP_KLEN],
                    #            (<Key>key_v.mv_data)[: KLEN]))
                    try:
                        _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        # Triple not found.
                        #logger.debug('spok / ck pair not found.')
                        return Keyset(0, TRP_KLEN)
                    ret = Keyset(1, TRP_KLEN)
                    memcpy(ret.data, spok, TRP_KLEN)

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
                        return Keyset(0, TRP_KLEN)

                    _check(lmdb.mdb_cursor_count(icur, &ct))
                    ret = Keyset(ct, TRP_KLEN)
                    logger.debug(f'Entries in c:spo: {ct}')
                    logger.debug(f'Allocated {ret.size} bytes.')

                    logger.debug('Looking in key: {}'.format(
                        (<unsigned char *>key_v.mv_data)[:key_v.mv_size]))
                    _check(lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
                    while True:
                        #logger.debug(f'Data offset: {pg_offset} Page size: {data_v.mv_size} bytes')
                        #logger.debug('Data page: {}'.format(
                        #        (<unsigned char *>data_v.mv_data)[: data_v.mv_size]))
                        memcpy(ret.data + pg_offset, data_v.mv_data, data_v.mv_size)
                        pg_offset += data_v.mv_size

                        try:
                            _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                        except KeyNotFoundError:
                            return ret

                # Regular lookup. Filter _lookup() results by context.
                else:
                    try:
                        res = self._lookup(triple_pattern)
                    except KeyNotFoundError:
                        return Keyset(0, TRP_KLEN)

                    #logger.debug('Allocating for context filtering.')
                    key_v.mv_data = ck
                    key_v.mv_size = KLEN
                    data_v.mv_size = TRP_KLEN

                    flt_res = Keyset(res.ct, res.itemsize)
                    while j < res.ct:
                        #logger.debug('Checking row #{}'.format(flt_j))
                        data_v.mv_data = res.data + j * res.itemsize
                        #logger.debug('Checking c:spo {}, {}'.format(
                        #    (<unsigned char *>key_v.mv_data)[: key_v.mv_size],
                        #    (<unsigned char *>data_v.mv_data)[: data_v.mv_size]))
                        try:
                            # Verify that the triple is associated with the
                            # context being searched.
                            _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                        except KeyNotFoundError:
                            #logger.debug('Discarding source[{}].'.format(j))
                            continue
                        else:
                            #logger.debug('Copying source[{}] to dest[{}].'.format(
                            #    j, flt_j))
                            memcpy(
                                    flt_res.data + res.itemsize * flt_j,
                                    res.data + res.itemsize * j, res.itemsize)

                            flt_j += 1
                        finally:
                            j += 1

                    # Resize result set to the size of context matches.
                    # This crops the memory block without copying it.
                    flt_res.resize(flt_j)
                    return flt_res
            finally:
                self._cur_close(icur)

        # Unfiltered lookup. No context checked.
        else:
            #logger.debug('No context in query.')
            try:
                res = self._lookup(triple_pattern)
            except KeyNotFoundError:
                return Keyset(0, TRP_KLEN)
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
            TripleKey spok
            lmdb.MDB_stat db_stat
            size_t ct = 0, i = 0
            lmdb.MDB_val spok_v, ck_v

        s, p, o = triple_pattern

        if s is not None:
            if p is not None:
                # s p o
                if o is not None:
                    spok_v.mv_data = spok
                    spok_v.mv_size = TRP_KLEN
                    try:
                        self._to_triple_key(triple_pattern, &spok)
                        _check(lmdb.mdb_get(
                            self.txn, self.get_dbi('spo:c'), &spok_v, &ck_v))
                    except KeyNotFoundError:
                        return Keyset(0, TRP_KLEN)

                    matches = Keyset(1, TRP_KLEN)
                    memcpy(matches.data, spok, TRP_KLEN)
                    return matches
                # s p ?
                else:
                    return self._lookup_2bound(0, s, 1, p)
            else:
                # s ? o
                if o is not None:
                    return self._lookup_2bound(0, s, 2, o)
                # s ? ?
                else:
                    return self._lookup_1bound(0, s)
        else:
            if p is not None:
                # ? p o
                if o is not None:
                    return self._lookup_2bound(1, p, 2, o)
                # ? p ?
                else:
                    return self._lookup_1bound(1, p)
            else:
                # ? ? o
                if o is not None:
                    return self._lookup_1bound(2, o)
                # ? ? ?
                else:
                    # Get all triples in the database.
                    logger.debug('Getting all DB triples.')
                    dcur = self._cur_open('spo:c')

                    try:
                        _check(lmdb.mdb_stat(
                                self.txn, lmdb.mdb_cursor_dbi(dcur), &db_stat),
                            'Error gathering DB stats.')
                        ct = db_stat.ms_entries
                        ret = Keyset(ct, TRP_KLEN)
                        logger.debug(f'Triples found: {ct}')
                        if ct == 0:
                            return Keyset(0, TRP_KLEN)

                        _check(lmdb.mdb_cursor_get(
                                dcur, &key_v, &data_v, lmdb.MDB_FIRST))
                        while True:
                            logger.debug(f'i in 0bound: {i}')
                            memcpy(
                                    ret.data + ret.itemsize * i,
                                    key_v.mv_data, TRP_KLEN)

                            try:
                                _check(lmdb.mdb_cursor_get(
                                    dcur, &key_v, &data_v, lmdb.MDB_NEXT_NODUP))
                            except KeyNotFoundError:
                                break

                            i += 1
                        # Size is guessed from all entries. Unique keys will be
                        # much less than that.
                        ret.resize(i + 1)

                        #logger.debug('Assembled data: {}'.format(ret.data[:ret.size]))
                        return ret
                    finally:
                        self._cur_close(dcur)


    cdef Keyset _lookup_1bound(self, unsigned char idx, term):
        """
        Lookup triples for a pattern with one bound term.

        :param str idx_name: The index to look up as one of the keys of
            ``_lookup_ordering``.
        :param rdflib.URIRef term: Bound term to search for.

        :rtype: Iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        cdef:
            unsigned char luk[KLEN]
            unsigned int dbflags
            unsigned char asm_rng[3]
            size_t ct, ret_offset = 0, src_pos, ret_pos
            size_t j # Must be signed for older OpenMP versions
            lmdb.MDB_cursor *icur

        logger.debug(f'lookup 1bound: {idx}, {term}')
        try:
            self._to_key(term, &luk)
        except KeyNotFoundError:
            return Keyset(0, TRP_KLEN)
        logging.debug('luk: {}'.format(luk))

        term_order = lookup_ordering[idx]
        icur = self._cur_open(self.lookup_indices[idx])
        logging.debug(f'DB label: {self.lookup_indices[idx]}')
        logging.debug('term order: {}'.format(term_order[: 3]))

        try:
            key_v.mv_data = luk
            key_v.mv_size = KLEN

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_count(icur, &ct))

            # Allocate memory for results.
            ret = Keyset(ct, TRP_KLEN)
            logger.debug(f'Entries for {self.lookup_indices[idx]}: {ct}')
            logger.debug(f'Allocated {ret.size} bytes of data.')
            #logger.debug('First row: {}'.format(
            #        (<unsigned char *>data_v.mv_data)[:DBL_KLEN]))

            # Arrange results according to lookup order.
            asm_rng = [
                KLEN * term_order[0],
                KLEN * term_order[1],
                KLEN * term_order[2],
            ]
            logger.debug('asm_rng: {}'.format(asm_rng[:3]))
            logger.debug('luk: {}'.format(luk))

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
            while True:
                logger.debug('ret_offset: {}'.format(ret_offset))
                logger.debug(f'Page size: {data_v.mv_size}')
                #logger.debug(
                #        'Got data in 1bound ({}): {}'.format(
                #            data_v.mv_size,
                #            (<unsigned char *>data_v.mv_data)[: data_v.mv_size]))
                for j in prange(data_v.mv_size // DBL_KLEN, nogil=True):
                    src_pos = DBL_KLEN * j
                    ret_pos = (ret_offset + ret.itemsize * j)
                    memcpy(ret.data + ret_pos + asm_rng[0], luk, KLEN)
                    memcpy(ret.data + ret_pos + asm_rng[1],
                            data_v.mv_data + src_pos, KLEN)
                    memcpy(ret.data + ret_pos + asm_rng[2],
                            data_v.mv_data + src_pos + KLEN, KLEN)

                # Increment MUST be done before MDB_NEXT_MULTIPLE otherwise
                # data_v.mv_size will be overwritten with the *next* page size
                # and cause corruption in the output data.
                ret_offset += data_v.mv_size // DBL_KLEN * ret.itemsize

                try:
                    # Get results by the page.
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                except KeyNotFoundError:
                    # For testing only. Errors will be caught in triples()
                    # when looking for a context.
                    #if ret_offset + ret.itemsize < ret.size:
                    #    raise RuntimeError(
                    #        'Retrieved less values than expected: {} of {}.'
                    #        .format(src_offset, ret.size))
                    return ret

            #logger.debug('Assembled data in 1bound ({}): {}'.format(ret.size, ret.data[: ret.size]))
        finally:
            self._cur_close(icur)


    cdef Keyset _lookup_2bound(
            self, unsigned char idx1, term1, unsigned char idx2, term2):
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
            unsigned char luk1[KLEN]
            unsigned char luk2[KLEN]
            unsigned char luk[DBL_KLEN]
            unsigned int dbflags
            unsigned char asm_rng[3]
            unsigned char term_order[3] # Lookup ordering
            size_t ct, i = 0, ret_offset = 0, ret_pos, src_pos
            size_t j # Must be signed for older OpenMP versions
            lmdb.MDB_cursor *icur
            Keyset ret

        logging.debug(
                f'2bound lookup for term {term1} at position {idx1} '
                f'and term {term2} at position {idx2}.')
        try:
            self._to_key(term1, &luk1)
            self._to_key(term2, &luk2)
        except KeyNotFoundError:
            return Keyset(0, TRP_KLEN)
        logging.debug('luk1: {}'.format(luk1[: KLEN]))
        logging.debug('luk2: {}'.format(luk2[: KLEN]))

        for i in range(3):
            if (
                    idx1 in lookup_ordering_2bound[i][: 2]
                    and idx2 in lookup_ordering_2bound[i][: 2]):
                term_order = lookup_ordering_2bound[i]
                if term_order[0] == idx1:
                    luk1_offset = 0
                    luk2_offset = KLEN
                else:
                    luk1_offset = KLEN
                    luk2_offset = 0
                dblabel = self.lookup_indices[i + 3] # skip 1bound index labels
                break

            if i == 2:
                raise ValueError(
                        'Indices {} and {} not found in LU keys.'.format(
                            idx1, idx2))

        logger.debug('Term order: {}'.format(term_order[:3]))
        logger.debug('LUK offsets: {}, {}'.format(luk1_offset, luk2_offset))
        # Compose terms in lookup key.
        memcpy(luk + luk1_offset, luk1, KLEN)
        memcpy(luk + luk2_offset, luk2, KLEN)

        logger.debug('Lookup key: {}'.format(luk))

        icur = self._cur_open(dblabel)
        logger.debug('Database label: {}'.format(dblabel))

        try:
            key_v.mv_data = luk
            key_v.mv_size = DBL_KLEN

            # Count duplicates for key and allocate memory for result set.
            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_count(icur, &ct))
            ret = Keyset(ct, TRP_KLEN)
            #logger.debug('Entries for {}: {}'.format(self.lookup_indices[idx], ct))
            #logger.debug('First row: {}'.format(
            #        (<unsigned char *>data_v.mv_data)[:DBL_KLEN]))

            # Arrange results according to lookup order.
            asm_rng = [
                KLEN * term_order[0],
                KLEN * term_order[1],
                KLEN * term_order[2],
            ]
            logger.debug('asm_rng: {}'.format(asm_rng[:3]))
            logger.debug('luk: {}'.format(luk))

            _check(lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET))
            _check(lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_GET_MULTIPLE))
            while True:
                logger.debug('Got data in 2bound ({}): {}'.format(
                    data_v.mv_size,
                    (<unsigned char *>data_v.mv_data)[: data_v.mv_size]))
                for j in prange(data_v.mv_size // KLEN, nogil=True):
                    src_pos = KLEN * j
                    ret_pos = (ret_offset + ret.itemsize * j)
                    #logger.debug('Page offset: {}'.format(pg_offset))
                    #logger.debug('Ret offset: {}'.format(ret_offset))
                    memcpy(ret.data + ret_pos + asm_rng[0], luk, KLEN)
                    memcpy(ret.data + ret_pos + asm_rng[1], luk + KLEN, KLEN)
                    memcpy(ret.data + ret_pos + asm_rng[2],
                            data_v.mv_data + src_pos, KLEN)
                    #logger.debug('Assembled triple: {}'.format((ret.data + ret_offset)[: TRP_KLEN]))

                ret_offset += data_v.mv_size // KLEN * ret.itemsize

                try:
                    # Get results by the page.
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_MULTIPLE))
                except KeyNotFoundError:
                    # For testing only. Errors will be caught in triples()
                    # when looking for a context.
                    #if ret_offset + ret.itemsize < ret.size:
                    #    raise RuntimeError(
                    #        'Retrieved less values than expected: {} of {}.'
                    #        .format(pg_offset, ret.size))
                    #logger.debug('Assembled data in 2bound ({}): {}'.format(ret.size, ret.data[: ret.size]))
                    return ret
        finally:
            self._cur_close(icur)


    cdef Keyset _all_term_keys(self, term_type):
        """
        Return all keys of a (``s:po``, ``p:so``, ``o:sp``) index.
        """
        cdef:
            size_t i = 0
            lmdb.MDB_stat stat

        idx_label = self.lookup_indices['spo'.index(term_type)]
        #logger.debug('Looking for all terms in index: {}'.format(idx_label))
        icur = self._cur_open(idx_label)
        try:
            _check(lmdb.mdb_stat(self.txn, lmdb.mdb_cursor_dbi(icur), &stat))
            # TODO: This may allocate memory for several times the amount
            # needed. Even though it is resized later, we need to know how
            # performance is affected by this.
            ret = Keyset(stat.ms_entries, KLEN)

            try:
                _check(lmdb.mdb_cursor_get(
                    icur, &key_v, NULL, lmdb.MDB_FIRST))
            except KeyNotFoundError:
                return Keyset(0, DBL_KLEN)

            while True:
                memcpy(ret.data + ret.itemsize * i, key_v.mv_data, KLEN)

                rc = lmdb.mdb_cursor_get(
                    icur, &key_v, NULL, lmdb.MDB_NEXT_NODUP)
                try:
                    _check(rc)
                except KeyNotFoundError:
                    ret.resize(i + 1)
                    return ret
                i += 1
        finally:
            #pass
            self._cur_close(icur)


    def all_terms(self, term_type):
        """
        Return all terms of a type (``s``, ``p``, or ``o``) in the store.
        """
        for key in self._all_term_keys(term_type).to_tuple():
            #logger.debug('Yielding: {}'.format(key))
            yield self.from_key(key)


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


    cpdef tuple all_contexts(self, triple=None):
        """
        Get a list of all contexts.

        :rtype: Iterator(lakesuperior.model.graph.graph.Imr)
        """
        cdef:
            lmdb.MDB_stat stat
            size_t i = 0
            unsigned char spok[TRP_KLEN]
            unsigned char ck[KLEN]
            lmdb.MDB_cursor_op op

        cur = (
                self._cur_open('spo:c') if triple and all(triple)
                else self._cur_open('c:'))
        try:
            if triple and all(triple):
                _check(lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(cur), &stat))
                ret = Keyset(stat.ms_entries, KLEN)

                self._to_triple_key(triple, &spok)
                key_v.mv_data = spok
                key_v.mv_size = TRP_KLEN
                try:
                    _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_SET_KEY))
                except KeyNotFoundError:
                    return tuple()

                while True:
                    memcpy(ret.data + ret.itemsize * i, data_v.mv_data, KLEN)
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                    except KeyNotFoundError:
                        break

                    i += 1
            else:
                _check(lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(cur), &stat))
                ret = Keyset(stat.ms_entries, KLEN)

                try:
                    _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_FIRST))
                except KeyNotFoundError:
                    return tuple()

                while True:
                    memcpy(
                        ret.data + ret.itemsize * i, key_v.mv_data, KLEN)
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, NULL, lmdb.MDB_NEXT))
                    except KeyNotFoundError:
                        break

                    i += 1

            return ret.to_tuple()

        finally:
            #pass
            self._cur_close(cur)


    # Key conversion methods.

    cdef object from_key(self, const Key key):
        """
        Convert a single key into one term.

        :param Key key: The key to be converted.
        """
        cdef Buffer pk_t

        self.lookup_term(key, &pk_t)

        return deserialize_to_rdflib(&pk_t)


    cdef inline void lookup_term(self, const Key key, Buffer* data) except *:
        """
        look up a term by key.

        :param Key key: The key to be looked up.
        :param Buffer *data: Buffer structure containing the serialized term.
        """
        cdef:
            lmdb.MDB_val key_v, data_v

        key_v.mv_data = key
        key_v.mv_size = KLEN

        logger.info('So far so good[0].')
        logger.info(f'Size of mdb_val: {sizeof(lmdb.MDB_val)}; size of buffer: {sizeof(Buffer)}')
        _check(
                lmdb.mdb_get(
                    self.txn, self.get_dbi('t:st'), &key_v, &data_v
                ),
                f'Error getting data for key \'{key}\'.')
        logger.info('So far so good[1].')
        data.addr = data_v.mv_data
        data.sz = data_v.mv_size
        logger.info('Found term: {}'.format(buffer_dump(data)))


    cdef tuple from_trp_key(self, TripleKey key):
        """
        Convert a triple key into a tuple of 3 terms.

        :param TripleKey key: The triple key to be converted.
        """
        #logger.debug(f'From triple key: {key[: TRP_KLEN]}')
        return (
                self.from_key(key),
                self.from_key(key + KLEN),
                self.from_key(key + DBL_KLEN))


    cdef inline void _to_key(self, term, Key *key) except *:
        """
        Convert a triple, quad or term into a key.

        The key is the checksum of the serialized object, therefore unique for
        that object.

        :param rdflib.Term term: An RDFLib term (URIRef, BNode, Literal).
        :param Key key: Pointer to the key that will be produced.

        :rtype: void
        """
        cdef:
            Hash128 thash
            Buffer pk_t

        serialize_from_rdflib(term, &pk_t)
        hash128(&pk_t, &thash)
        #logger.debug('Hash to search for: {}'.format(thash[: HLEN]))
        key_v.mv_data = thash
        key_v.mv_size = HLEN

        dbi = self.get_dbi('th:t')
        logger.debug(f'DBI: {dbi}')
        _check(lmdb.mdb_get(self.txn, dbi, &key_v, &data_v))
        #logger.debug('Found key: {}'.format((<Key>data_v.mv_data)[: KLEN]))

        key[0] = <Key>data_v.mv_data


    cdef inline void _to_triple_key(
            self, tuple terms, TripleKey *tkey) except *:
        """
        Convert a tuple of 3 terms into a triple key.
        """
        cdef:
            unsigned char i = 0
            Key key

        while  i < 3:
            self._to_key(terms[i], &key)
            memcpy(tkey[0] + (KLEN * i), key, KLEN)
            if key is NULL:
                # A term in the triple is not found.
                tkey = NULL
                return
            i += 1


    cdef void _append(
            self, Buffer *value, Key *nkey,
            unsigned char *dblabel=b'', lmdb.MDB_txn *txn=NULL,
            unsigned int flags=0) except *:
        """
        Append one or more keys and values to the end of a database.

        :param lmdb.Cursor cur: The write cursor to act on.
        :param list(bytes) values: Value(s) to append.

        :rtype: list(memoryview)
        :return: Last key(s) inserted.
        """
        cdef:
            unsigned char key[KLEN]
            lmdb.MDB_cursor *cur

        if txn is NULL:
            txn = self.txn

        cur = self._cur_open(dblabel, txn=txn)

        try:
            _check(lmdb.mdb_cursor_get(cur, &key_v, NULL, lmdb.MDB_LAST))
        except KeyNotFoundError:
            memcpy(nkey[0], FIRST_KEY, KLEN)
        else:
            memcpy(key, key_v.mv_data, KLEN)
            self._next_key(key, nkey)
        finally:
            #pass
            self._cur_close(cur)

        key_v.mv_data = nkey
        key_v.mv_size = KLEN
        data_v.mv_data = value[0].addr
        data_v.mv_size = value[0].sz
        #logger.debug('Appending value {} to db {} with key: {}'.format(
        #    value[: vlen], dblabel.decode(), nkey[0][:KLEN]))
        #logger.debug('data size: {}'.format(data_v.mv_size))
        lmdb.mdb_put(
                txn, self.get_dbi(dblabel), &key_v, &data_v,
                flags | lmdb.MDB_APPEND)


    cdef void _next_key(self, const Key key, Key *nkey) except *:
        """
        Calculate the next closest byte sequence in lexicographical order.

        This is used to fill the next available slot after the last one in
        LMDB. Keys are byte strings, which is a convenient way to keep key
        lengths as small as possible since they are referenced in several
        indices.

        This function assumes that all the keys are padded with the `start`
        value up to the `max_len` length.

        :param bytes n: Current byte sequence to add to.
        """
        cdef:
            size_t i = KLEN

        memcpy(nkey[0], key, KLEN)

        #logger.debug('Last key in _next_key: {}'.format(key[0]))
        while i > 0:
            i -= 1
            if nkey[0][i] < 255:
                nkey[0][i] += 1
                break
            # If the value exceeds 255, i.e. the current value is the last one
            else:
                # If we are already at the leftmost byte, and this is already
                # at 255, the sequence is exhausted.
                if i == 0:
                    raise RuntimeError(
                            'BAD DAY: Sequence exhausted. No more '
                            'combinations are possible.')
                # Move one position up and try to increment that.
                else:
                    nkey[0][i] = KEY_START
        #logger.debug('New key: {}'.format(nkey[0][:KLEN]))
