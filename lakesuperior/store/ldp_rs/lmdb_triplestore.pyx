# cython: language_level = 3

import hashlib
import logging

from collections import OrderedDdict

from lakesuperior.store.base_lmdb_store import BaseLmdbStore, LmdbError

from libc cimport errno

from lakesuperior.cy_include cimport cylmdb as lmdb

logger = loggin.getLogger(__name__)


cdef class LexicalSequence:
    """
    Fixed-length lexicographically ordered byte sequence.

    Useful to generate optimized sequences of keys in LMDB.
    """
    def __init__(self, start=1, max_len=5):
        """
        Create a new lexical sequence.

        :param bytes start: Starting byte value. Bytes below this value are
            never found in this sequence. This is useful to allot special bytes
            to be used e.g. as separators.
        :param int max_len: Maximum number of bytes that a byte string can
            contain. This should be chosen carefully since the number of all
            possible key combinations is determined by this value and the
            ``start`` value. The default args provide 255**5 (~1 Tn) unique
            combinations.
        """
        self.start = start
        self.length = max_len


    cdef bytes first(self):
        """First possible combination."""
        return chr(self.start).encode() * self.length


    cdef bytes next(self, n):
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
        if not n:
            n = self.first()
        elif isinstance(n, bytes) or isinstance(n, memoryview):
            n = bytearray(n)
        elif not isinstance(n, bytearray):
            raise ValueError('Input sequence must be bytes or a bytearray.')

        if not len(n) == self.length:
            raise ValueError('Incorrect sequence length.')

        for i, b in list(enumerate(n))[::-1]:
            try:
                n[i] += 1
            # If the value exceeds 255, i.e. the current value is the last one
            except ValueError:
                if i == 0:
                    raise RuntimeError('BAD DAY: Sequence exhausted. No more '
                            'combinations are possible.')
                # Move one position up and try to increment that.
                else:
                    n[i] = self.start
                    continue
            else:
                return bytes(n)



cdef class LmdbTriplestore(BaseLmdbStore):

    db_config = OrderedDict((
        # Main data
        # Term key to serialized term content
        ('t:st', 0),
        # Joined triple keys to context key
        ('spo:c', 0),
        # This has empty values and is used to keep track of empty contexts.
        ('c:', 0),
        # Prefix to namespace
        ('pfx:ns', 0),

        # Indices
        # Namespace to prefix
        ('ns:pfx', 0),
        # Term hash to triple key
        ('th:t', 0),
        # Lookups
        ('s:po', lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED),
        ('p:so', lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED),
        ('o:sp', lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED),
        ('c:spo', lmdb.MDB_DUPSORT | lmdb.MDB_DUPFIXED),
    ))

    flags = lmdb.MDB_NOSUBDIR | lmdb.MDB_NORDAHEAD

    options = {
        'map_size': 1024 ** 4 # 1Tb.
    }
