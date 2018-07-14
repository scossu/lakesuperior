# cython: language_level = 3

import hashlib
import logging
import os
import pickle

from lakesuperior.store.base_lmdb_store import LmdbError
from lakesuperior.store.base_lmdb_store cimport _check

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

DEF LOOKUP_RANK = 'sop'
"""
Order in which keys are looked up if two terms are bound.
The indices with the smallest average number of values per key should be
looked up first.

If we want to get fancy, this can be rebalanced from time to time by
looking up the number of keys in (s:po, p:so, o:sp).
"""


ctypedef unsigned char Key[KLEN]
ctypedef unsigned char DoubleKey[DBL_KLEN]
ctypedef unsigned char TripleKey[TRP_KLEN]
ctypedef unsigned char QuadKey[QUAD_KLEN]
ctypedef unsigned char Hash[HLEN]


cdef unsigned char lookup_ordering[3][3]
lookup_ordering = [
    [0, 1, 2],
    [1, 0, 2],
    [2, 0, 1],
]


cdef inline void _hash(const unsigned char[:] s, Hash *ch):
    """Get the hash value of a serialized object."""
    h = hashlib.new(TERM_HASH_ALGO, s).digest()
    ch[0] = <Hash>hash


logger = logging.getLogger(__name__)


def TstoreKeyNotFoundError(LmdbError):
    pass


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
                        rc, 'Error setting key on context {}: {{}}'.format(
                            context))
                    _check(
                        lmdb.mdb_cursor_count(cur, &ct),
                        'Error counting dup values for key {}: {{}}'.format(
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
                    'Error setting key {}: {{}}'.format(thash))
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


    # Key conversion methods.

    cdef _from_key(self, Key key):
        """
        Convert a key into one term.

        :param Key key: The key to be converted.
        """
        thash = <Hash>self._get_data(key, 't:st')
        return self._unpickle(thash)


    cdef void _to_key(self, term, Key *key) except *:
        """
        Convert a triple, quad or term into a key.

        The key is the checksum of the pickled object, therefore unique for
        that object. The hashing algorithm is specified in `TERM_HASH_ALGO`.

        :param Object obj: Anything that can be reduced to terms stored in the
        database. Pairs of terms, as well as triples and quads, are expressed
        as tuples.

        If more than one term is provided, the keys are concatenated.

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
                'Error getting data for key \'{}\': {{}}'.format(key[0]))

            key = <Key *>data_v.mv_data


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
                'Error getting data for key \'{}\': {{}}'.format(key))

            return data_v.mv_data


    cdef void _append(
            self, str dbi, const unsigned char *value, Key *lastkey,
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
            _check(rc, 'Error retrieving last key for DB {}: {{}}'.format(dbi))
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
