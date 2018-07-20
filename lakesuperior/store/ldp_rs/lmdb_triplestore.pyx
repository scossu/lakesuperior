# cython: language_level = 3

import hashlib
import logging
import os
import pickle

from lakesuperior.store.base_lmdb_store import LmdbError
from lakesuperior.store.base_lmdb_store cimport _check

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc cimport errno

from lakesuperior.cy_include cimport cylmdb as lmdb
from lakesuperior.store.base_lmdb_store cimport (
        BaseLmdbStore, data_v, dbi, key_v)

DEF KLEN = 5
"""
Fixed length for term keys.

4 or 5 is a safe range. 4 allows for ~4 billion (256 ** 4) unique terms
in the store. 5 allows ~1 trillion terms. While these numbers may seem
huge (the total number of Internet pages indexed by Google as of 2018 is 45
billions), it must be reminded that the keys cannot be reused, so a
repository that deletes a lot of triples may burn through a lot of terms.

If a repository runs ot of keys it can no longer store new terms and must
be migrated to a new database, which will regenerate and compact the keys.

For smaller repositories it should be safe to set this value to 4, which
could improve performance since keys make up the vast majority of record
exchange between the store and the application. However it is sensible not
to expose this value as a configuration option.
"""

DEF DBL_KLEN = KLEN * 2
DEF TRP_KLEN = KLEN * 3
DEF QUAD_KLEN = KLEN * 4


DEF TERM_HASH_ALGO = 'sha1'
""" Term hashing algorithm. SHA1 is the default. """

DEF HLEN = 20
""" Hash byte length. For SHA1 this is 20. """

DEF KEY_START = b'\x01'
"""
Lexical sequence start. ``\\x01`` is fine since no special characters are
used, but it's good to leave a spare for potential future use.
"""

DEF FIRST_KEY = KEY_START * KLEN
"""First key of a sequence."""


ctypedef unsigned char Key[KLEN]
ctypedef unsigned char DoubleKey[DBL_KLEN]
ctypedef unsigned char TripleKey[TRP_KLEN]
ctypedef unsigned char QuadKey[QUAD_KLEN]
ctypedef unsigned char Hash[HLEN]


cdef unsigned char lookup_rank[3]
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

cdef unsigned char lookup_ordering[3][3]
lookup_ordering = [
    [0, 1, 2], # spo
    [1, 0, 2], # pso
    [2, 0, 1], # osp
]


cdef inline void _hash(const unsigned char[:] s, Hash *ch):
    """Get the hash value of a serialized object."""
    h = hashlib.new(TERM_HASH_ALGO, s).digest()
    ch[0] = <Hash>hash


logger = logging.getLogger(__name__)


def TstoreKeyNotFoundError(LmdbError):
    pass


cdef class ResultSet:
    """
    Pre-allocated result set.
    """
    cdef:
        unsigned char **data
        size_t size, itemsize

    def __cinit__(self, size_t size, size_t itemsize):
        self.data = <unsigned char **>PyMem_Malloc(size * itemsize)
        if not self.data:
            raise MemoryError()
        self.size = size
        self.itemsize = itemsize

    def __dealloc__(self):
        PyMem_Free(self.data)

    cdef void resize(self, size_t size) except *:
        cdef unsigned char **mem
        mem = <unsigned char **>PyMem_Realloc(self.data, size * self.itemsize)
        if not mem:
            raise MemoryError()
        self.data = mem
        self.size = size

    cdef tuple to_tuple(self):
        """
        Return the data set as a tuple.
        """
        return tuple(x for x in self.data[:self.size])



cdef class LmdbTriplestore(BaseLmdbStore):

    _pickle = pickle.dumps
    _unpickle = pickle.loads

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
        'c:spo',
    ]

    lookup_indices = [
        's:po',
        'p:so',
        'o:sp',
    ]

    dbi_flags = {
        's:po': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'p:so': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'o:sp': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
        'c:spo': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
    }

    flags = lmdb.MDB_NOSUBDIR | lmdb.MDB_NORDAHEAD

    options = {
        'map_size': 1024 ** 4 # 1Tb.
    }


    # DB management methods.

    def stats(self):
        """Gather statistics about the database."""
        stats = super().stats()
        stats['num_triples'] = stats['db_stats']['spo:c']['entries']

        return stats

    def _len(self, context=None):
        """
        Return the length of the dataset.

        The RDFLib interface defines `__len__` in a nonstandard way that
        causes a Cython compilation error, so this method is called by the
        `__len__` method of its Python counterpart.
        """
        cdef:
            size_t ct
            lmdb.MDB_cursor *cur

        if context is not None:
            self._to_key(context, <Key *>key_v.mv_data)
            key_v.mv_size = KLEN

            with self.txn_ctx:
                try:
                    cur = self._cur_open(self.txn, 'c:spo')
                    rc = lmdb.mdb_cursor_get(cur, &key_v, NULL, lmdb.MDB_SET)
                    if rc == lmdb.MDB_NOTFOUND:
                        return 0
                    _check(
                        rc, 'Error setting key on context {}.'.format(
                            context))
                    _check(
                        lmdb.mdb_cursor_count(cur, &ct),
                        'Error counting dup values for key {}.'.format(
                            key_v.mv_data[0]))
                finally:
                    self._cur_close(cur)
        else:
            return self.stats()['num_triples']


    ## PRIVATE METHODS ##

    # Triple and graph methods.

    cdef void _add(
            self, const unsigned char[:] pk_s,
            const unsigned char[:] pk_p,
            const unsigned char[:] pk_o,
            const unsigned char[:] pk_c) except *:
        """
        Add a triple and start indexing.

        :param tuple(rdflib.Identifier) triple: Tuple of three identifiers.
        :param context: Context identifier. ``None`` inserts in the default
            graph.
        :type context: rdflib.Identifier or None
        :param bool quoted: Not used.
        """
        cdef:
            Hash thash
            #const unsigned char[:] *pk_terms = [pk_s, pk_p, pk_o, pk_c]
            lmdb.MDB_cursor *dcur
            lmdb.MDB_cursor *icur
            QuadKey keys
            char i
            # For some reason, using Key or TripleKey here breaks Cython.
            unsigned char spok[TRP_KLEN]
            unsigned char ck[KLEN]
            Key tkey

        icur = self._cur_open(self.txn, 'th:t')
        dcur = self._cur_open(self.txn, 't:st')
        key_v.mv_size = HLEN
        data_v.mv_size = KLEN

        for i, pk_t in enumerate((pk_s, pk_p, pk_o, pk_c)):
            _hash(pk_t[0], &thash)
            try:
                keys[KLEN * i: KLEN * (i + 1)] = \
                        <unsigned char *>self._get_data(thash, 'th:t')
            except TstoreKeyNotFoundError:
                # If term is not found, add it...
                self._append('t:st', pk_t, &tkey)
                keys[KLEN * i: KLEN * (i + 1)] = tkey
                # ...and index it.
                key_v.mv_data = thash
                data_v.mv_data = tkey
                _check(
                    lmdb.mdb_cursor_put(dcur, &key_v, &data_v, 0),
                    'Error setting key {}.'.format(thash))
        self._cur_close(dcur)
        self._cur_close(icur)

        # Add context.
        ck = keys[TRP_KLEN: QUAD_KLEN]
        self.put(ck, b'', 'c:', lmdb.MDB_NOOVERWRITE)

        # Add triple:context association.
        spok = keys[: TRP_KLEN]
        self.put(spok, ck, 'spo:c', lmdb.MDB_NOOVERWRITE)
        # Index triple:context association.
        self.put(ck, spok, 'c:spo', lmdb.MDB_NOOVERWRITE)

        self._index_triple('add', spok)


    #cdef void _delete(triple_pattern, ck):
    #    dcur = self._cur_open('spo:c')
    #    icur = self._cur_open('c:spo')

    #    self._cur_close(dcur)
    #    self._cur_close(icur)
    #    match_set = self._triple_keys(triple_pattern, ck)

    #    # # #
    #    with self.cur('spo:c') as dcur:
    #        with self.cur('c:spo') as icur:
    #            match_set = {bytes(k) for k in self._triple_keys(
    #                    triple_pattern, context)}
    #            # Delete context association.
    #            if ck:
    #                for spok in match_set:
    #                    if dcur.set_key_dup(spok, ck):
    #                        dcur.delete()
    #                        if icur.set_key_dup(ck, spok):
    #                            icur.delete()
    #                        self._index_triple('remove', spok)
    #            # If no context is specified, remove all associations.
    #            else:
    #                for spok in match_set:
    #                    if dcur.set_key(spok):
    #                        for cck in (bytes(k) for k in dcur.iternext_dup()):
    #                            # Delete index first while we have the
    #                            # context reference.
    #                            if icur.set_key_dup(cck, spok):
    #                                icur.delete()
    #                        # Then delete the main entry.
    #                        dcur.set_key(spok)
    #                        dcur.delete(dupdata=True)
    #                        self._index_triple('remove', spok)


    # Lookup methods.

    cpdef tuple _triple_keys(self, tuple triple_pattern, context=None):
        """
        Top-level (for this class) lookup method.

        This method is used by `triples` which returns native Python tuples,
        as well as by other methods that need to iterate and filter triple
        keys without incurring in the overhead of converting them to triples.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph or URI, or None.
        :type context: rdflib.term.Identifier or None
        """
        cdef:
            unsigned char tk[KLEN]
            unsigned char ck[KLEN]
            unsigned char spok[TRP_KLEN]
            size_t ct = 0, flt_ct = 0
            Py_ssize_t i = 0
            lmdb.MDB_cursor *icur

        if context is not None:
            pk_c = self._pickle(context)
            self._to_key(context, &ck)

            # Shortcuts
            if ck is NULL:
                # Context not found.
                return tuple()

            icur = self._cur_open(self.txn, 'c:spo')
            key_v.mv_data = ck
            key_v.mv_size = KLEN

            # s p o c
            if all(triple_pattern):
                for i, term in enumerate(triple_pattern):
                    self._to_key(term, &tk)
                    spok[KLEN * i: KLEN * (i + 1)] = tk
                    if tk is NULL:
                        # A term in the triple is not found.
                        self._cur_close(icur)
                        return tuple()
                data_v.mv_data = spok
                data_v.mv_size = TRP_KLEN
                rc = lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_GET_BOTH)
                if rc == lmdb.MDB_NOTFOUND:
                    # Triple not found.
                    self._cur_close(icur)
                    return tuple()
                _check(rc, 'Error getting key + data pair.')
                self._cur_close(icur)
                return (spok,)

            # ? ? ? c
            elif not any(triple_pattern):
                # Get all triples from the context
                rc = lmdb.mdb_cursor_get(icur, &key_v, NULL, lmdb.MDB_SET)
                if rc == lmdb.MDB_NOTFOUND:
                    # Triple not found.
                    self._cur_close(icur)
                    return tuple()

                _check(lmdb.mdb_cursor_count(icur, &ct),
                        'Error counting values.')
                ret = ResultSet(ct, TRP_KLEN)
                while (lmdb.mdb_cursor_get(
                    icur, &key_v, &data_v, lmdb.MDB_NEXT_DUP
                ) == lmdb.MDB_SUCCESS):
                    ret.data[i] = <TripleKey>data_v.mv_data
                    i += 1
                    self._cur_close(icur)

                    return ret.to_tuple()

            # Regular lookup. Filter _lookup() results by context.
            else:
                res = self._lookup(triple_pattern)
                if res.size == 0:
                    return tuple()

                flt_res = ResultSet(res.size, res.itemsize)
                while flt_ct < res.size:
                    data_v.mv_data = res.data[flt_ct]
                    rc = lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_GET_BOTH)
                    if rc == lmdb.MDB_SUCCESS:
                        flt_res.data[flt_ct] = res.data[flt_ct]
                    flt_ct += 1

                flt_res.resize(flt_ct)
        # Unfiltered lookup. No context checked.
        else:
            return self._lookup(triple_pattern).to_tuple()


    cdef ResultSet _lookup(self, triple_pattern):
        """
        Look up triples in the indices based on a triple pattern.

        :rtype: Iterator
        :return: Matching triple keys.
        """
        cdef:
            TripleKey spok
            lmdb.MDB_stat db_stat
            size_t ct = 0
            Py_ssize_t i = 0
        s, p, o = triple_pattern

        if s is not None:
            if p is not None:
                # s p o
                if o is not None:
                    self._to_triple_key(triple_pattern, &spok)
                    if spok is not NULL:
                        matches = ResultSet(1, TRP_KLEN)
                        matches.data = [spok]
                        return matches
                    else:
                        matches = ResultSet(0, TRP_KLEN)
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
                    dcur = self._cur_open(self.txn, 'spo:c')
                    _check(lmdb.mdb_stat(
                            self.txn, self._get_dbi('spo:c'), &db_stat),
                        'Error gathering DB stats.')
                    ct = db_stat.ms_entries
                    res = ResultSet(<int>ct, TRP_KLEN)
                    while lmdb.mdb_cursor_get(
                        dcur, NULL, &data_v, lmdb.MDB_NEXT
                    ) == lmdb.MDB_SUCCESS:
                        res.data[i] = <Key>data_v.mv_data
                        i += 1
                    self._cur_close(dcur)

                    return res


    cdef ResultSet _lookup_1bound(self, unsigned char idx, term):
        """
        Lookup triples for a pattern with one bound term.

        :param str idx_name: The index to look up as one of the keys of
            ``_lookup_ordering``.
        :param rdflib.URIRef term: Bound term to search for.

        :rtype: Iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        cdef:
            unsigned char k[KLEN]
            size_t ct, i = 0
            unsigned char match[DBL_KLEN]
            unsigned char subkey1[KLEN]
            unsigned char subkey2[KLEN]
            unsigned int asm_rng[3][2]
            unsigned char _asm_key[TRP_KLEN]

        self._to_key(term, &k)
        if k is NULL:
            return ResultSet(0, TRP_KLEN)

        term_order = self._lookup_ordering[idx]
        icur = self._cur_open(self.txn, self.lookup_indices[idx])
        key_v.mv_data = k
        key_v.mv_size = KLEN
        _check(
                lmdb.mdb_cursor_get(icur, &key_v, NULL, lmdb.MDB_SET),
                'Error getting resource count key.')
        _check(
                lmdb.mdb_cursor_count(icur, &ct),
                'Error getting resource count.')

        # Allocate memory for results.
        matches = ResultSet(ct, TRP_KLEN)
        if ct > 0:
            # Arrange results according to lookup order.
            asm_rng = [
                [KLEN * term_order[0], KLEN * (term_order[0] + 1)],
                [KLEN * term_order[1], KLEN * (term_order[1] + 1)],
                [KLEN * term_order[2], KLEN * (term_order[2] + 1)],
            ]

            while lmdb.mdb_cursor_get(
                icur, &key_v, &data_v, lmdb.MDB_NEXT_DUP
            ) == lmdb.MDB_SUCCESS:
                match = <unsigned char *>data_v.mv_data
                subkey1 = match[:KLEN]
                subkey2 = match[KLEN: DBL_KLEN]
                _asm_key[asm_rng[0][0]: asm_rng[0][1]] = k
                _asm_key[asm_rng[1][0]: asm_rng[1][1]] = subkey1
                _asm_key[asm_rng[2][0]: asm_rng[2][1]] = subkey2
                matches.data[i] = _asm_key
                i += 1
        return matches


    cdef ResultSet _lookup_2bound(
            self, unsigned char idx1, term1, unsigned char idx2, term2):
        """
        Look up triples for a pattern with two bound terms.

        :param  bound: terms (dict) Triple labels and terms to search for,
        in the format of, e.g. {'s': URIRef('urn:s:1'), 'o':
        URIRef('urn:o:1')}

        :rtype: iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        cdef:
            unsigned char fkp, ftl
            unsigned char subkey_range[2]
            unsigned char asm_rng[3][2]
            unsigned char luk[KLEN]
            unsigned char fk[KLEN]
            unsigned char rk[KLEN]
            unsigned char match[DBL_KLEN]
            size_t ct
            Py_ssize_t i = 0
            #Key luk, fk, rk

        # First we need to get:
        # - ludbl: The lookup DB label (s:po, p:so, o:sp)
        # - luk: The KLEN-byte lookup key
        # - ftl: The filter term label (s, p, o)
        # - fk: The KLEN-byte filter key
        # - fkp: The position of filter key in a (s, p, o) term (0÷2)
        # These have to be selected in the order given by lookup_rank.
        ludbl = None # Lookup DB label: s:po, p:so, or o:sp
        for lui in lookup_rank: # Lookup index: s, p, or o
            if lui == idx1 or lui == idx2:
                lut = term1 if lui == idx1 else term2
                luti = 'spo'.index(lui) # Lookup term index: 0÷2
                # First match is used to find the lookup DB.
                if ludbl is None:
                    #v_label = self.lookup_indices[luti]
                    # Lookup database key (cursor) name
                    ludbl = self.lookup_indices[luti]
                    term_order = lookup_ordering[luti]
                    # Term to look up (lookup key)
                    self._to_key(lut, &luk)
                    if luk is NULL:
                        return ResultSet(0, TRP_KLEN)
                # Second match is the filter.
                else:
                    # Filter key (position of sub-key in lookup results)
                    fkp = ludbl.split(':')[1].index(lui)
                    # Fliter term
                    self._to_key(lut, &fk)
                    if fk is NULL:
                        return ResultSet(0, TRP_KLEN)
                    break
            # The index that does not match idx1 or idx2 is the unbound one.

        # Precompute the array slices that are used in the loop.
        flt_subkey_rng = [KLEN * fkp, KLEN * (fkp + 1)]
        r_subkey_rng = [KLEN * (1 - fkp), KLEN * (2 - fkp)]
        asm_rng = [
            [KLEN * term_order[0], KLEN * (term_order[0] + 1)],
            [KLEN * (fkp + 1), KLEN * (fkp + 2)],
            [KLEN * (2 - fkp), KLEN * (1 - fkp)],
        ]

        # Now Look up in index.
        icur = self._cur_open(self.txn, ludbl)
        key_v.mv_data = luk
        key_v.mv_size = KLEN
        rc = lmdb.mdb_cursor_get(icur, &key_v, NULL, lmdb.MDB_SET)

        if rc == lmdb.MDB_NOTFOUND:
            return ResultSet(0, TRP_KLEN)

        _check(rc, 'Error getting 2bound lookup key.')
        _check(
                lmdb.mdb_cursor_count(icur, &ct),
                'Error getting 2bound term count.')
        # Initially allocate memory for the maximum possible matches,
        # it will be resized later.
        matches = ResultSet(ct, TRP_KLEN)
        # Iterate over matches and filter by second term.
        while (
                lmdb.mdb_cursor_get(
                    icur, &key_v, &data_v, lmdb.MDB_NEXT_DUP)
                == lmdb.MDB_SUCCESS):
            match = <DoubleKey>data_v.mv_data

            if match[flt_subkey_rng[0]: flt_subkey_rng[1]] == fk:
                # Remainder (not filter) key to complete the triple.
                rk = match[r_subkey_rng[0]: r_subkey_rng[1]]

                # Assemble result.
                matches.data[i][asm_rng[0][0]: asm_rng[0][1]] = luk
                matches.data[i][asm_rng[1][0]: asm_rng[1][1]] = fk
                matches.data[i][asm_rng[2][0]: asm_rng[2][1]] = rk
            i += 1

        # Shrink the array to the actual number of matches.
        matches.resize(i)
        self._cur_close(icur)

        return matches


    # Key conversion methods.

    cdef object _from_key(self, Key key):
        """
        Convert a key into one term.

        :param Key key: The key to be converted.
        """
        thash = <Hash>self._get_data(key, 't:st')
        return self._unpickle(thash)


    cdef inline void _to_key(self, term, Key *key) except *:
        """
        Convert a triple, quad or term into a key.

        The key is the checksum of the pickled object, therefore unique for
        that object. The hashing algorithm is specified in `TERM_HASH_ALGO`.

        :param Object obj: Anything that can be reduced to terms stored in the
        database. Pairs of terms, as well as triples and quads, are expressed
        as tuples.

        :rtype: memoryview or None
        :return: Keys stored for the term(s) or None if not found.
        """
        cdef Hash thash
        _hash(self._pickle(term), &thash)
        key_v.mv_data = thash
        key_v.mv_size = HLEN

        dbi = self.get_dbi('th:t')[0]
        with self.txn_ctx():
            rc = lmdb.mdb_get(self.txn, dbi, &key_v, &data_v)
            if rc == lmdb.MDB_NOTFOUND:
                raise TstoreKeyNotFoundError()
            _check(rc,
                'Error getting data for key \'{}\'.'.format(key[0]))

            key = <Key *>data_v.mv_data


    cdef inline void _to_triple_key(self, tuple terms, TripleKey *tkey) except *:
        """
        Convert a tuple of 3 terms into a triple key.
        """
        cdef:
            char i = 0
            Key key

        while  i < 3:
            self._to_key(terms[i], &key)
            tkey[0][KLEN * i: KLEN * (i + 1)] = key
            if key is NULL:
                # A term in the triple is not found.
                tkey = NULL
                return
            i += 1


    cdef void *_get_data(self, Key key, str db):
        """
        Get a single value (non-dup) for a key.
        """
        cdef:
            unsigned char[:] ret

        key_v.mv_data = key
        key_v.mv_size = KLEN

        dbi = self.get_dbi(db)[0]
        with self.txn_ctx():
            rc = lmdb.mdb_get(self.txn, dbi, &key_v, &data_v)
            if rc == lmdb.MDB_NOTFOUND:
                raise TstoreKeyNotFoundError()
            _check(rc,
                'Error getting data for key \'{}\'.'.format(key))

            return data_v.mv_data


    cdef void _append(
            self, str dbi, unsigned char *value, Key *lastkey,
            unsigned int flags=0) except *:
        """
        Append one or more keys and values to the end of a database.

        :param lmdb.Cursor cur: The write cursor to act on.
        :param list(bytes) values: Value(s) to append.

        :rtype: list(memoryview)
        :return: Last key(s) inserted.
        """
        cdef:
            lmdb.MDB_cursor *cur = self._cur_open(self.txn, dbi)
            Key nkey

        rc = lmdb.mdb_cursor_get(cur, &key_v, NULL, lmdb.MDB_LAST)
        if rc == lmdb.MDB_NOTFOUND:
            lastkey[0] = FIRST_KEY
        else:
            _check(rc, 'Error retrieving last key for DB {}.'.format(dbi))
            lastkey = <Key *>key_v.mv_data
        key_v.mv_size = KLEN
        self._next_key(lastkey, &nkey)
        key_v.mv_data = nkey
        data_v.mv_data = value
        data_v.mv_size = len(value)
        lmdb.mdb_put(
                self.txn, self.get_dbi(dbi)[0], &key_v, &data_v,
                flags | lmdb.MDB_APPEND)


    cdef void _next_key(self, Key *key, Key *nkey) except *:
        """
        Calculate the next closest byte sequence in lexicographical order.

        This is used to fill the next available slot after the last one in
        LMDB. Keys are byte strings, which is a convenient way to keep key
        lengths as small as possible when they are referenced in several
        indices.

        This function assumes that all the keys are padded with the `start`
        value up to the `max_len` length.

        :param bytes n: Current byte sequence to add to.
        """
        cdef:
            size_t i = KLEN

        nkey[0] = key[0]

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
                    raise RuntimeError('BAD DAY: Sequence exhausted. No more '
                            'combinations are possible.')
                # Move one position up and try to increment that.
                else:
                    nkey[0][i] = KEY_START
