# cython: language_level = 3
# cython: boundschecking = False
# cython: wraparound = False

import hashlib
import logging
import os
import pickle

from lakesuperior.store.base_lmdb_store import (
        KeyExistsError, KeyNotFoundError, LmdbError)
from lakesuperior.store.base_lmdb_store cimport _check

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.string cimport memcmp, memcpy, strchr

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

DEF IDX_OP_ADD = '_idx_add'
DEF IDX_OP_REMOVE = '_idx_remove'


ctypedef unsigned char Key[KLEN]
ctypedef unsigned char DoubleKey[DBL_KLEN]
ctypedef unsigned char TripleKey[TRP_KLEN]
ctypedef unsigned char QuadKey[QUAD_KLEN]
ctypedef unsigned char Hash[HLEN]


cdef unsigned char first_key[KLEN]
memcpy(first_key, FIRST_KEY, KLEN)

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


cdef inline void _hash(const unsigned char *s, Py_ssize_t size, Hash *ch):
    """Get the hash value of a serialized object."""
    htmp = hashlib.new(TERM_HASH_ALGO, s[: size]).digest()
    ch[0] = <unsigned char *>htmp


logger = logging.getLogger(__name__)


cdef class ResultSet:
    """
    Pre-allocated result set.

    Data in the set are stored as a 1D contiguous array of characters.
    Access to elements at an arbitrary index position is achieved by using the
    ``itemsize`` property multiplied by the index number.

    Key properties:

    ``ct``: number of elements in the set.
    ``itemsize``: size of each element, in bytes. All elements have the same
        size.
    ``size``: Total size, in bytes, of the data set. This is the product of
        ``itemsize`` and ``ct``.
    """
    cdef:
        unsigned char *data
        unsigned char itemsize
        size_t ct, size

    def __cinit__(self, size_t ct, unsigned char itemsize):
        """
        Initialize and allocate memory for the data set.

        :param size_t ct: Number of elements to be accounted for.
        :param unsigned char itemsize: Size of an individual item.
            Note that the ``itemsize`` is an unsigned char,
            i.e. an item can be at most 255 bytes. This is for economy reasons,
            since many multiplications are done between ``itemsize`` and other
            char variables.
        """
        self.data = <unsigned char *>PyMem_Malloc(ct * itemsize)
        if not self.data:
            raise MemoryError()
        self.ct = ct
        self.itemsize = itemsize
        self.size = self.itemsize * self.ct

        logger.debug('Dimensions of allocated ResultSet data: {}x{}'.format(
            self.ct, self.itemsize))
        logger.debug('Size of allocated ResultSet data: {}'.format(
            self.size))
        logger.debug('Memory address of allocated data: {0:x}'.format(
            <unsigned long>self.data))


    def __dealloc__(self):
        logger.debug('Releasing {} bytes of ResultSet...'.format(
            self.ct * self.itemsize))
        PyMem_Free(self.data)
        logger.debug('...done.')


    cdef void resize(self, size_t ct) except *:
        cdef unsigned char *tmp
        tmp = <unsigned char *>PyMem_Realloc(self.data, ct * self.itemsize)
        if not tmp:
            raise MemoryError()
        self.data = tmp
        self.ct = ct
        self.size = self.itemsize * self.ct

    # Access methods.

    cdef tuple to_tuple(self):
        """
        Return the data set as a Python tuple.
        """
        return tuple(
                self.data[i: i + self.itemsize]
                for i in range(0, self.size, self.itemsize))


    cdef unsigned char *get_item(self, i):
        """
        Get an item at a given index position.

        The item size is known by the ``itemsize`` property of the object.
        """
        return self.data + self.itemsize * i


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
        'spo:c': lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED,
    }

    flags = lmdb.MDB_NOSUBDIR | lmdb.MDB_NORDAHEAD

    options = {
        'map_size': 1024 ** 4 # 1Tb.
    }


    # DB management methods.

    cpdef dict stats(self, new_txn=True):
        """Gather statistics about the database."""
        st = self._stats()
        st['num_triples'] = st['db_stats']['spo:c']['entries']

        return st


    def _len(self, context=None):
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
                return self.stats()['num_triples']
            finally:
                self._cur_close(cur)


    ## PRIVATE METHODS ##

    # Triple and graph methods.

    cpdef void _add(
            self, bytes pk_s,
            bytes pk_p,
            bytes pk_o,
            bytes pk_c) except *:
        """
        Add a triple and start indexing.

        :param tuple(rdflib.Identifier) triple: Tuple of three identifiers.
        :param context: Context identifier. ``None`` inserts in the default
            graph.
        :type context: rdflib.Identifier or None
        :param bool quoted: Not used.
        """
        cdef:
            #const unsigned char[:] *pk_terms = [pk_s, pk_p, pk_o, pk_c]
            lmdb.MDB_cursor *icur
            lmdb.MDB_val spo_v, c_v, null_v
            unsigned char i
            unsigned char *pk_t
            unsigned char thash[HLEN]
            # Using Key or TripleKey here breaks Cython. This might be a bug.
            # See https://github.com/cython/cython/issues/2517
            unsigned char keys[QUAD_KLEN]
            unsigned char spok[TRP_KLEN]
            unsigned char ck[KLEN]
            unsigned char tkey[KLEN]
            unsigned char nkey[KLEN]
            unsigned int term_sizes[4]

        logger.info('Calculating term sizes.')
        term_sizes = [len(pk_s), len(pk_p), len(pk_o), len(pk_c)]

        logger.debug('Pickled context: {}'.format(pk_c))
        logger.debug('Term sizes: {}'.format(term_sizes))

        icur = self._cur_open('th:t')
        try:
            for i, pk_t in enumerate((pk_s, pk_p, pk_o, pk_c)):
                logger.debug('Pickled term: {}'.format(pk_t[: term_sizes[i]]))
                logger.debug('Pickled term size: {}'.format(term_sizes[i]))
                _hash(pk_t, term_sizes[i], &thash)
                try:
                    key_v.mv_data = &thash
                    key_v.mv_size = HLEN
                    _check(lmdb.mdb_get(
                            self.txn, self.get_dbi('th:t'), &key_v, &data_v))
                    memcpy(keys + (i * KLEN), data_v.mv_data, KLEN)
                    logger.debug('Hash {} found.'.format(thash[: HLEN]))
                except KeyNotFoundError:
                    # If term is not found, add it...
                    logger.debug('Hash {} not found. Adding to DB.'.format(
                            thash[: HLEN]))
                    self._append('t:st', pk_t, term_sizes[i], tkey, &nkey)
                    memcpy(keys + (i * KLEN), nkey, KLEN)

                    # ...and index it.
                    logger.debug('Indexing on th:t: {}: {}'.format(
                            thash[: HLEN], nkey[: KLEN]))
                    key_v.mv_data = thash
                    key_v.mv_size = HLEN
                    data_v.mv_data = nkey
                    data_v.mv_size = KLEN
                    logger.debug('DB key length: {}'.format(key_v.mv_size))
                    _check(
                        lmdb.mdb_cursor_put(icur, &key_v, &data_v, 0),
                        'Error setting key {}.'.format(thash))
        finally:
            self._cur_close(icur)

        # Add context.
        # TODO We can avoid 2 memcpy's by copying directly to the destinations.
        memcpy(ck, keys + TRP_KLEN, KLEN)
        memcpy(spok, keys, TRP_KLEN)

        spo_v.mv_data = spok
        spo_v.mv_size = TRP_KLEN
        c_v.mv_data = ck
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
            # Index triple:context association.
            _check(lmdb.mdb_put(
                self.txn, self.get_dbi('c:spo'), &c_v, &spo_v,
                lmdb.MDB_NODUPDATA))
        except KeyExistsError:
            pass

        self._index_triple(IDX_OP_ADD, spok)


    cpdef void _remove(self, tuple triple_pattern, context=None):
        cdef:
            unsigned char spok[TRP_KLEN]
            size_t i = 0
            Key ck
            lmdb.MDB_val spok_v, ck_v

        if context is not None:
            try:
                self._to_key(context, &ck)
            except KeyNotFoundError:
                # If context is specified but not found, return to avoid
                # deleting the wrong triples.
                return

        # Get the matching pattern.
        match_set = self._triple_keys(triple_pattern, context)

        dcur = self._cur_open('spo:c')
        icur = self._cur_open('c:spo')

        try:
            # If context was specified, remove only associations with that context.
            if context is not None:
                ck_v.mv_data = ck
                ck_v.mv_size = KLEN
                spok_v.mv_size = TRP_KLEN
                while i < match_set.ct:
                    memcpy(spok, match_set.data + match_set.itemsize * i, TRP_KLEN)
                    spok_v.mv_data = spok
                    if lmdb.mdb_cursor_get(
                            dcur, &spok_v, &ck_v, lmdb.MDB_GET_BOTH
                    ) == lmdb.MDB_SUCCESS:
                        _check(
                            lmdb.mdb_cursor_del(dcur, 0),
                            'Error deleting main entry.')
                        if lmdb.mdb_cursor_get(
                                icur, &ck_v, &spok_v, lmdb.MDB_GET_BOTH
                        ) == lmdb.MDB_SUCCESS:
                            _check(
                                lmdb.mdb_cursor_del(icur, 0),
                                'Error deleting index entry.')
                        self._index_triple(IDX_OP_REMOVE, spok)
                    i += 1

            # If no context is specified, remove all associations.
            else:
                spok_v.mv_size = TRP_KLEN
                # Loop over all SPO matching the triple pattern.
                while i < match_set.ct:
                    memcpy(spok, match_set.data + match_set.itemsize * i, TRP_KLEN)
                    spok_v.mv_data = spok
                    # Loop over all context associations for this SPO.
                    while lmdb.mdb_cursor_get(
                            dcur, &spok_v, &ck_v, lmdb.MDB_NEXT_DUP
                    ) == lmdb.MDB_SUCCESS:
                        if lmdb.mdb_cursor_get(
                                icur, &ck_v, &spok_v, lmdb.MDB_GET_BOTH
                        ) == lmdb.MDB_SUCCESS:
                            # Delete index first while we have the
                            # context reference.
                            lmdb.mdb_cursor_del(icur, 0)
                    # Then delete the main entry.
                    if lmdb.mdb_cursor_get(
                            dcur, &spok_v, NULL, lmdb.MDB_SET
                    ) == lmdb.MDB_SUCCESS:
                        lmdb.mdb_cursor_del(icur, lmdb.MDB_NODUPDATA)
                        self._index_triple(IDX_OP_REMOVE, spok)
                    i += 1

        finally:
            self._cur_close(icur)
            self._cur_close(dcur)


    cdef void _index_triple(self, str op, TripleKey spok) except *:
        """
        Update index for a triple and context (add or remove).

        :param str op: 'add' or 'remove'.
        :param TripleKey spok: Triple key.
        """
        cdef:
            unsigned char keys[3][KLEN]
            unsigned char data[3][DBL_KLEN]
            Py_ssize_t i = 0

        memcpy(&keys[0], spok, KLEN) # sk
        memcpy(&keys[1], spok + KLEN, KLEN) # pk
        memcpy(&keys[2], spok +DBL_KLEN, KLEN) # ok

        memcpy(&data[0], spok + KLEN, DBL_KLEN) # pok
        memcpy(&data[1], spok, KLEN) # sok, 1st part
        memcpy(&data[1][KLEN], spok + DBL_KLEN, KLEN) # sok, 2nd part
        memcpy(&data[2], spok, DBL_KLEN) # spk
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
        #        data[0][:DBL_KLEN], data[1][:DBL_KLEN], data[2][:DBL_KLEN]))
        key_v.mv_size = KLEN
        data_v.mv_size = DBL_KLEN

        while i < 3:
            icur = self._cur_open(self.lookup_indices[i])
            try:
                key_v.mv_data = keys[i]
                data_v.mv_data = data[i]

                if op == IDX_OP_REMOVE:
                    if lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_GET_BOTH
                    ) == lmdb.MDB_SUCCESS:
                        _check(lmdb.mdb_cursor_del(icur, 0))
                elif op == IDX_OP_ADD:
                    rc = lmdb.mdb_cursor_put(
                            icur, &key_v, &data_v, lmdb.MDB_NODUPDATA)
                    # Do not raise on MDB_KEYEXIST error code.
                    if rc != lmdb.MDB_KEYEXIST:
                        _check(rc)
                else:
                    raise ValueError(
                        'Index operation \'{}\' is not supported.'.format(op))
                i += 1
            finally:
                self._cur_close(icur)


    # Lookup methods.

    cpdef get_dup_data(self, key, dblabel=None):
        """
        Get all duplicate values for a key. Python-facing method.
        """
        logger.debug('Go fetch dup data for key: {} in DB: {}'.format(key, dblabel))
        ret = self._get_dup_data(key, len(key), dblabel).to_tuple()
        logger.debug('Dup data as tuple: {}'.format(ret))
        return ret


    cdef ResultSet _get_dup_data(
            self, unsigned char *key, unsigned char ksize, dblabel=None):
        """
        Get all duplicate values for a key.
        """
        logger.debug('In _get_dup_data: key: {} in DB: {}'.format(key[: ksize], dblabel))
        cdef:
            size_t ct, i = 0
            unsigned int dbflags
            ResultSet ret
            lmdb.MDB_cursor *cur

        logger.debug('DB label: {}'.format(dblabel))
        key_v.mv_data = key
        key_v.mv_size = ksize
        logger.debug('Key: {}'.format(key[: ksize]))
        logger.debug('Key size: {}'.format(ksize))

        cur = self._cur_open(dblabel)
        try:
            _check(lmdb.mdb_dbi_flags(
                self.txn, lmdb.mdb_cursor_dbi(cur), &dbflags))
            if not lmdb.MDB_DUPFIXED & dbflags or not lmdb.MDB_DUPSORT & dbflags:
                raise ValueError('This DB is not set up with fixed values.')

            rc = lmdb.mdb_cursor_get(cur, &key_v, &data_v, lmdb.MDB_SET)
            try:
                _check(rc)
            except KeyNotFoundError:
                return ResultSet(0, 0)

            _check(lmdb.mdb_cursor_count(cur, &ct))
            ret = ResultSet(ct, data_v.mv_size)
            logger.debug('array sizes: {}x{}'.format(ret.ct, ret.itemsize))

            while True:
                memcpy(
                        ret.data + i * ret.itemsize, data_v.mv_data,
                        ret.itemsize)
                logger.debug('Data in row: {}'.format(
                    ret.data[ret.itemsize * i: ret.itemsize * (i + 1)]))

                rc = lmdb.mdb_cursor_get(
                    cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP)
                try:
                    _check(rc)
                except KeyNotFoundError:
                    break

                i += 1

            logger.debug('Total data in _get_dup_data: {}'.format(
                ret.data[: ret.size]))
            return ret
        finally:
            self._cur_close(cur)


    cpdef tuple triple_keys(self, tuple triple_pattern, context=None):
        """
        Python-facing method that returns results as a tuple.
        """
        ret = self._triple_keys(triple_pattern, context)
        logger.debug('All triple keys: {}'.format(ret.data[: ret.size]))
        logger.debug('Triples as tuple: {}'.format(ret.to_tuple()))
        return ret.to_tuple()


    cdef ResultSet _triple_keys(self, tuple triple_pattern, context=None):
        """
        Top-level Cython-facing lookup method.

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
            ResultSet flt_res, ret

        if context is not None:
            pk_c = self._pickle(context)
            try:
                self._to_key(context, &ck)
            except KeyNotFoundError:
                # Context not found.
                return ResultSet(0, TRP_KLEN)

            icur = self._cur_open('c:spo')
            try:
                key_v.mv_data = ck
                key_v.mv_size = KLEN

                # s p o c
                if all(triple_pattern):
                    for i, term in enumerate(triple_pattern):
                        self._to_key(term, &tk)
                        memcpy(spok + (KLEN * i), tk, KLEN)
                        if tk is NULL:
                            # A term in the triple is not found.
                            self._cur_close(icur)
                            return ResultSet(0, TRP_KLEN)
                    data_v.mv_data = spok
                    data_v.mv_size = TRP_KLEN
                    try:
                        _check(lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH))
                    except KeyNotFoundError:
                        # Triple not found.
                        return ResultSet(0, TRP_KLEN)
                    finally:
                        self._cur_close(icur)
                    ret = ResultSet(1, TRP_KLEN)
                    memcpy(ret.data, spok, TRP_KLEN)

                    return ret

                # ? ? ? c
                elif not any(triple_pattern):
                    # Get all triples from the context
                    try:
                        _check(lmdb.mdb_cursor_get(
                            icur, &key_v, NULL, lmdb.MDB_SET))
                    except KeyNotFoundError:
                        # Triple not found.
                        self._cur_close(icur)
                        return ResultSet(0, TRP_KLEN)

                    _check(lmdb.mdb_cursor_count(icur, &ct),
                            'Error counting values.')
                    ret = ResultSet(ct, TRP_KLEN)
                    while (lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_NEXT_DUP
                    ) == lmdb.MDB_SUCCESS):
                        memcpy(ret.data + ret.itemsize * i, data_v.mv_data, TRP_KLEN)
                        i += 1
                    self._cur_close(icur)

                    return ret

                # Regular lookup. Filter _lookup() results by context.
                else:
                    res = self._lookup(triple_pattern)
                    if res.ct == 0:
                        return ResultSet(0, TRP_KLEN)

                    flt_res = ResultSet(res.ct, res.itemsize)
                    while flt_ct < res.ct:
                        data_v.mv_data = res.data + flt_ct * res.itemsize
                        rc = lmdb.mdb_cursor_get(
                                icur, &key_v, &data_v, lmdb.MDB_GET_BOTH)
                        if rc == lmdb.MDB_SUCCESS:
                            memcpy(
                                    flt_res.data + res.itemsize * flt_ct,
                                    res.data + res.itemsize * flt_ct, res.itemsize)

                        flt_ct += 1

                    flt_res.resize(flt_ct * res.itemsize)
            finally:
                self._cur_close(icur)
        # Unfiltered lookup. No context checked.
        else:
            res = self._lookup(triple_pattern)
            logger.debug('Res data before _triple_keys return: {}'.format(res.data[: res.size]))
            return res


    cdef ResultSet _lookup(self, tuple triple_pattern):
        """
        Look up triples in the indices based on a triple pattern.

        :rtype: Iterator
        :return: Matching triple keys.
        """
        cdef:
            TripleKey spok
            lmdb.MDB_stat db_stat
            size_t ct = 0, i = 0
        s, p, o = triple_pattern

        if s is not None:
            if p is not None:
                # s p o
                if o is not None:
                    self._to_triple_key(triple_pattern, &spok)
                    if spok is not NULL:
                        matches = ResultSet(1, TRP_KLEN)
                        memcpy(matches.data, spok, TRP_KLEN)
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
                    dcur = self._cur_open('spo:c')
                    try:
                        rc = lmdb.mdb_cursor_get(
                                dcur, &key_v, &data_v, lmdb.MDB_FIRST)
                        try:
                            _check(rc)
                        except KeyNotFoundError:
                            return ResultSet(0, TRP_KLEN)

                        _check(lmdb.mdb_stat(
                                self.txn, lmdb.mdb_cursor_dbi(dcur), &db_stat),
                            'Error gathering DB stats.')
                        ct = db_stat.ms_entries
                        ret = ResultSet(ct, TRP_KLEN)

                        while True:
                            memcpy(
                                    ret.data + ret.itemsize * i,
                                    key_v.mv_data, TRP_KLEN)

                            rc = lmdb.mdb_cursor_get(
                                dcur, &key_v, &data_v, lmdb.MDB_NEXT)
                            try:
                                _check(rc)
                            except KeyNotFoundError:
                                break

                            i += 1

                        return ret
                    finally:
                        self._cur_close(dcur)


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
            unsigned char luk[KLEN]
            unsigned int dbflags
            unsigned char asm_rng[3]
            size_t ct, i = 0

        self._to_key(term, &luk)
        if luk is NULL:
            return ResultSet(0, TRP_KLEN)
        logging.debug('luk: {}'.format(luk))

        term_order = lookup_ordering[idx]
        icur = self._cur_open(self.lookup_indices[idx])
        logging.debug('term order: {}'.format(term_order))
        try:
            key_v.mv_data = luk
            key_v.mv_size = KLEN
            rc = lmdb.mdb_cursor_get(icur, &key_v, &data_v, lmdb.MDB_SET)

            _check(lmdb.mdb_dbi_flags(
                self.txn, lmdb.mdb_cursor_dbi(icur), &dbflags))
            if not lmdb.MDB_DUPFIXED & dbflags or not lmdb.MDB_DUPSORT & dbflags:
                raise ValueError('This DB is not set up with fixed values.')

            try:
                _check(rc)
            except KeyNotFoundError:
                return ResultSet(0, TRP_KLEN)

            _check(lmdb.mdb_cursor_count(icur, &ct))

            # Allocate memory for results.
            ret = ResultSet(ct, TRP_KLEN)
            logger.debug('Entries for {}: {}'.format(self.lookup_indices[idx], ct))
            logger.debug('First row: {}'.format(
                    (<unsigned char *>data_v.mv_data)[:DBL_KLEN]))

            # Arrange results according to lookup order.
            asm_rng = [
                KLEN * term_order[0],
                KLEN * term_order[1],
                KLEN * term_order[2],
            ]
            logger.debug('asm_rng: {}'.format(asm_rng[:3]))
            logger.debug('luk: {}'.format(luk))

            while True:
                logger.debug('i: {}'.format(i))
                memcpy(ret.data + ret.itemsize * i + asm_rng[0], luk, KLEN)
                memcpy(
                        ret.data + ret.itemsize * i + asm_rng[1],
                        data_v.mv_data, KLEN)
                memcpy(
                        ret.data + ret.itemsize * i + asm_rng[2],
                        data_v.mv_data + KLEN, KLEN)

                logger.debug('Data: {}'.format(
                    ret.data[ret.itemsize * i: ret.itemsize * (i + 1)]))

                rc = lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_NEXT_DUP)
                try:
                    _check(rc)
                except KeyNotFoundError:
                    return ret

                i += 1

        finally:
            self._cur_close(icur)


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
        logger.debug('idx1: {} idx2: {}'.format(idx1, idx2))
        logger.debug('term1: {} term2: {}'.format(term1, term2))
        cdef:
            unsigned char fk_rng, fkp, lui, rk_rng, rkp
            unsigned char term_order[3]
            unsigned char asm_rng[3]
            unsigned char luk[KLEN]
            unsigned char fk[KLEN]
            unsigned char rk[KLEN]
            unsigned char match[DBL_KLEN]
            size_t i = 0, ct
            ResultSet ret

        # First we need to get:
        # - ludbl: The lookup DB label (s:po, p:so, o:sp)
        # - luk: The KLEN-byte lookup key
        # - fk: The KLEN-byte filter key
        # - fkp: The position of filter key in a (s, p, o) term (0÷2)
        # - rkp: The position of unbound key in a (s, p, o) term (0÷2)
        # These have to be selected in the order given by lookup_rank.
        logger.debug('Begin 2bound setup.')
        ludbl = None # Lookup DB label: s:po, p:so, or o:sp
        while i < 3:
            logger.debug('i: {}'.format(i))
            lui = lookup_rank[i] # Lookup term index: 0÷2
            logger.debug('term index handled (lui): {}'.format('spo'[lui]))
            if lui == idx1 or lui == idx2:
                term = term1 if lui == idx1 else term2
                logger.debug('Bound term being handled: {}'.format(term))
                #luti = b'spo'[lui]
                #logger.debug('luti: {}'.format(luti))
                # First match is used to find the lookup DB.
                if ludbl is None:
                    logger.debug('ludbl is None. Looking for lookup key.')
                    #v_label = self.lookup_indices[luti]
                    # Lookup database key (cursor) name
                    ludbl = self.lookup_indices[lui]
                    term_order = lookup_ordering[lui][:3]
                    logger.debug('term order: {}'.format(term_order[:3]))
                    # Term to look up (lookup key)
                    self._to_key(term, &luk)
                    if luk is NULL:
                        return ResultSet(0, TRP_KLEN)
                    logger.debug('Lookup key (luk): {}'.format(luk[: KLEN]))
                # Second match is the filter.
                else:
                    logger.debug('ludbl is {}. Looking for filter key.'.format(
                        ludbl))

                    # Precompute the array slice points that are used in
                    # the loop.

                    # Filter key position (sub-key position in lookup results)
                    # This is either 0 or 1
                    if term_order[1] == lui:
                        fkp = 0
                        rkp = 1
                    else:
                        fkp = 1
                        rkp = 0
                    logger.debug('Filter subkey pos in result data: {}'.format(fkp))
                    logger.debug('Remainder (unbound) subkey pos in result data: {}'.format(rkp))
                    # Fliter term
                    self._to_key(term, &fk)
                    if fk is NULL:
                        return ResultSet(0, TRP_KLEN)
                    logger.debug('Filter key (fk): {}'.format(fk[: KLEN]))
                    break
            # The index that does not match idx1 or idx2 is the unbound one.
            i += 1

        # Precompute pointer arithmetic outside of the loop.
        asm_rng = [
            KLEN * term_order[0],
            KLEN * term_order[fkp + 1],
            KLEN * term_order[rkp + 1],
        ]
        logger.debug('asm_rng: {}'.format(asm_rng[:3]))
        fk_rng = KLEN * fkp
        rk_rng = KLEN * rkp

        # Now Look up in index.
        icur = self._cur_open(ludbl)
        try:
            key_v.mv_data = luk
            key_v.mv_size = KLEN
            try:
                _check(lmdb.mdb_cursor_get(
                        icur, &key_v, &data_v, lmdb.MDB_SET))
            except KeyNotFoundError:
                return ResultSet(0, TRP_KLEN)

            _check(
                    lmdb.mdb_cursor_count(icur, &ct),
                    'Error getting 2bound term count.')
            # Initially allocate memory for the maximum possible ret,
            # it will be resized later.
            ret = ResultSet(ct, TRP_KLEN)
            # Iterate over ret and filter by second term.
            i = 0
            while True:
                logger.debug('Match: {}'.format(
                        (<unsigned char *>data_v.mv_data)[: DBL_KLEN]))
                logger.debug('fk: {}'.format(fk[: KLEN]))
                if memcmp(data_v.mv_data + fk_rng, fk, KLEN) == 0:
                    logger.debug('Assembling results.')
                    # Assemble result.
                    memcpy(ret.data + ret.itemsize * i + asm_rng[0], luk, KLEN)
                    memcpy(ret.data + ret.itemsize * i + asm_rng[1], fk, KLEN)
                    # Remainder (unbound key) to complete the triple.
                    memcpy(
                        ret.data + ret.itemsize * i + asm_rng[2],
                        data_v.mv_data + rk_rng, KLEN)

                    logger.debug('Assembled match: {}'.format(
                        ret.data[ret.itemsize * i: ret.itemsize * (i + 1)]))
                    i += 1

                try:
                    _check(lmdb.mdb_cursor_get(
                            icur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                except KeyNotFoundError:
                    break

            # Shrink the array to the actual number of ret.
            logger.debug('Resizing results to {} size.'.format(i))
            ret.resize(i)

            logger.debug('Returning results from lookup_2bound.')
            return ret
        finally:
            self._cur_close(icur)


    cdef ResultSet _all_term_keys(self, term_type):
        """
        Return all keys of a (``s:po``, ``p:so``, ``o:sp``) index.
        """
        cdef:
            Py_ssize_t i = 0
            lmdb.MDB_stat stat

        idx_label = self.lookup_indices['spo'.index(term_type)]
        icur = self._cur_open(idx_label)
        try:
            _check(lmdb.mdb_stat(self.txn, lmdb.mdb_cursor_dbi(icur), &stat))
            ret = ResultSet(stat.ms_entries, KLEN)

            try:
                _check(lmdb.mdb_cursor_get(
                    icur, &key_v, &data_v, lmdb.MDB_SET))
            except KeyNotFoundError:
                return ResultSet(0, DBL_KLEN)

            while True:
                memcpy(ret.data + ret.itemsize * i, key_v.mv_data, KLEN)

                rc = lmdb.mdb_cursor_get(
                    icur, &key_v, NULL, lmdb.MDB_NEXT_NODUP)
                try:
                    _check(rc)
                except KeyNotFoundError:
                    return ret
                i += 1
        finally:
            self._cur_close(icur)


    def all_terms(self, term_type):
        """
        Return all terms of a type (``s``, ``p``, or ``o``) in the store.
        """
        for key in self._all_term_keys(term_type):
            yield self._from_key(key, KLEN)[0]


    def all_namespaces(self):
        """
        Return all registered namespaces.
        """
        cdef:
            Py_ssize_t i = 0
            lmdb.MDB_stat stat

        ret = []
        dcur = self._cur_open('pfx:ns')
        try:
            try:
                _check(lmdb.mdb_cursor_get(
                    dcur, &key_v, &data_v, lmdb.MDB_SET))
            except KeyNotFoundError:
                return ResultSet(0, DBL_KLEN)

            _check(lmdb.mdb_stat(self.txn, lmdb.mdb_cursor_dbi(dcur), &stat))
            ret = ResultSet(stat.ms_entries, KLEN)

            while True:
                ret.append((<str>key_v.mv_data, <str>data_v.mv_data))
                try:
                    _check(lmdb.mdb_cursor_get(
                        dcur, &key_v, &data_v, lmdb.MDB_NEXT))
                except KeyNotFoundError:
                    return ret

                i += 1
        finally:
            self._cur_close(dcur)


    cpdef tuple all_contexts(self, triple=None):
        """
        Get a list of all contexts.

        :rtype: Iterator(rdflib.Graph)
        """
        cdef:
            lmdb.MDB_stat stat
            Py_ssize_t i = 0
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
                ret = ResultSet(stat.ms_entries, KLEN)

                try:
                    _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_SET_KEY))
                except KeyNotFoundError:
                    return tuple()

                self._to_triple_key(triple, &spok)
                key_v.mv_data = spok
                key_v.mv_size = TRP_KLEN
                while True:
                    memcpy(ret.data + ret.itemsize * i, data_v.mv_data, KLEN)
                    try:
                        _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_NEXT_DUP))
                    except KeyNotFoundError:
                        break
            else:
                _check(lmdb.mdb_stat(
                    self.txn, lmdb.mdb_cursor_dbi(cur), &stat))
                ret = ResultSet(stat.ms_entries, KLEN)

                try:
                    _check(lmdb.mdb_cursor_get(
                            cur, &key_v, &data_v, lmdb.MDB_SET_KEY))
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

            return ret.to_tuple()

        finally:
            self._cur_close(cur)


    # Key conversion methods.

    def from_key(self, key):
        logger.debug('Received Key in Python method: {} Size: {}'.format(key, len(key)))
        ret = self._from_key(key, len(key))
        logger.debug('Ret in public function: {}'.format(ret))
        return ret


    cdef tuple _from_key(self, unsigned char *key, Py_ssize_t size):
        """
        Convert a single or multiple key into one or more terms.

        :param Key key: The key to be converted.
        """
        cdef:
            unsigned char *pk_t
            Py_ssize_t i
            unsigned char subkey[KLEN]

        ret = []
        logger.debug('Find term from key: {}'.format(key[: size]))
        for i in range(0, size, KLEN):
            memcpy(subkey, key + i, KLEN)
            key_v.mv_data = &subkey
            key_v.mv_size = KLEN

            _check(
                    lmdb.mdb_get(
                        self.txn, self.get_dbi('t:st'), &key_v, &data_v),
                    'Error getting data for key \'{}\'.'.format(key))

            pk_t = <unsigned char *>PyMem_Malloc(data_v.mv_size)
            if pk_t == NULL:
                raise MemoryError()
            try:
                memcpy(pk_t, data_v.mv_data, data_v.mv_size)
                pk = bytes(pk_t[: data_v.mv_size])
                py_term = pickle.loads(pk)
                ret.append(py_term)
            finally:
                PyMem_Free(pk_t)
        logger.debug('Ret: {}'.format(ret))

        return tuple(ret)


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
        pk_t = self._pickle(term)
        _hash(pk_t, len(pk_t), &thash)
        key_v.mv_data = &thash
        key_v.mv_size = HLEN

        _check(
                lmdb.mdb_get(self.txn, self.get_dbi('th:t'), &key_v, &data_v),
                'Error getting data for key \'{}\'.'.format(key[0]))

        memcpy(key, data_v.mv_data, KLEN)


    cdef inline void _to_triple_key(self, tuple terms, TripleKey *tkey) except *:
        """
        Convert a tuple of 3 terms into a triple key.
        """
        cdef:
            char i = 0
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
            self, str dblabel, unsigned char *value, size_t vlen,
            const Key key, Key *nkey, unsigned int flags=0) except *:
        """
        Append one or more keys and values to the end of a database.

        :param lmdb.Cursor cur: The write cursor to act on.
        :param list(bytes) values: Value(s) to append.

        :rtype: list(memoryview)
        :return: Last key(s) inserted.
        """
        cdef:
            lmdb.MDB_cursor *cur = self._cur_open(dblabel)

        try:
            _check(lmdb.mdb_cursor_get(cur, &key_v, NULL, lmdb.MDB_LAST))
        except KeyNotFoundError:
            memcpy(nkey[0], first_key, KLEN)
        else:
            memcpy(key, key_v.mv_data, KLEN)
            self._next_key(key, nkey)
        finally:
            self._cur_close(cur)

        key_v.mv_data = nkey
        key_v.mv_size = KLEN
        data_v.mv_data = value
        data_v.mv_size = vlen
        logger.debug('Appending value {} to db {} with key: {}'.format(
            value[: vlen], dblabel, nkey[0][:KLEN]))
        logger.debug('data size: {}'.format(data_v.mv_size))
        lmdb.mdb_put(
                self.txn, self.get_dbi(dblabel), &key_v, &data_v,
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
                print('Incrementing value: {}'.format(nkey[0][i]))
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
        logger.debug('New key: {}'.format(nkey[0][:KLEN]))
