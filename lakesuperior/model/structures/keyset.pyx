import logging

from libc.string cimport memcmp, memcpy
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

cimport lakesuperior.model.structures.callbacks as cb

from lakesuperior.model.base cimport NULL_TRP, TRP_KLEN, TripleKey


logger = logging.getLogger(__name__)


cdef class Keyset:
    """
    Pre-allocated array (not set, as the name may suggest) of ``TripleKey``s.
    """
    def __cinit__(self, size_t capacity=0, expand_ratio=.5):
        """
        Initialize and allocate memory for the data set.

        :param size_t capacity: Number of elements to be accounted for.
        """
        self.capacity = capacity
        self.expand_ratio = expand_ratio
        self.data = <TripleKey*>PyMem_Malloc(self.capacity * TRP_KLEN)
        if capacity and not self.data:
            raise MemoryError('Error allocating Keyset data.')

        self._cur = 0
        self._free_i = 0


    def __dealloc__(self):
        """
        Free the memory.

        This is called when the Python instance is garbage collected, which
        makes it handy to safely pass a Keyset instance across functions.
        """
        PyMem_Free(self.data)


    # Access methods.

    cdef void seek(self, size_t idx=0):
        """
        Place the cursor at a certain index, 0 by default.
        """
        self._cur = idx


    cdef size_t size(self):
        """
        Size of the object as the number of occupied data slots.

        Note that this is different from :py:data:`capacity`_, which indicates
        the number of allocated items in memory.
        """
        return self._free_i


    cdef size_t tell(self):
        """
        Tell the position of the cursor in the keyset.
        """
        return self._cur


    cdef bint get_next(self, TripleKey* val):
        """
        Populate the current value and advance the cursor by 1.

        :param void *val: Addres of value returned. It is NULL if
            the end of the buffer was reached.

        :rtype: bint
        :return: True if a value was found, False if the end of the buffer
            has been reached.
        """
        if self._cur >= self._free_i:
            return False

        val[0] = self.data[self._cur]
        self._cur += 1

        return True


    cdef void add(self, const TripleKey* val, bint check_dup=False) except *:
        """
        Add a triple key to the array.
        """
        # Check for deleted triples and optionally duplicates.
        if val[0] == NULL_TRP or (check_dup and self.contains(val)):
            return

        if self._free_i >= self.capacity:
            if self.expand_ratio > 0:
                # In some edge casees, a very small ratio may round down to a
                # zero increase, so the baseline increase is 1 element.
                self.resize(1 + <size_t>(self.capacity * (1 + self.expand_ratio)))
            else:
                raise MemoryError('No space left in key set.')

        self.data[self._free_i] = val[0]

        self._free_i += 1


    cdef void remove(self, const TripleKey* val) except *:
        """
        Remove a triple key.

        This method replaces a triple with NULL_TRP if found. It
        does not reclaim space. Therefore, if many removal operations are
        forseen, using :py:meth:`subtract`_ is advised.
        """

        cdef TripleKey* stored_val

        self.seek()
        while self.get_next(stored_val):
            if memcmp(val, stored_val, TRP_KLEN) == 0:
                stored_val[0] = NULL_TRP
                return


    cdef bint contains(self, const TripleKey* val):
        """
        Whether a value exists in the set.
        """
        cdef TripleKey stored_val

        self.seek()
        while self.get_next(&stored_val):
            if memcmp(val, stored_val, TRP_KLEN) == 0:
                return True
        return False


    cdef Keyset copy(self):
        """
        Copy a Keyset.
        """
        cdef Keyset new_ks = Keyset(self.capacity, expand_ratio=self.expand_ratio)
        memcpy(new_ks.data, self.data, self.capacity * TRP_KLEN)
        new_ks.seek()

        return new_ks


    cdef Keyset sparse_copy(self):
        """
        Copy a Keyset and plug holes.

        ``NULL_TRP`` values left from removing triple keys are skipped in the
        copy and the set is shrunk to its used size.
        """
        cdef:
            TripleKey val
            Keyset new_ks = Keyset(self.capacity, self.expand_ratio)

        self.seek()
        while self.get_next(&val):
            if val != NULL_TRP:
                new_ks.add(&val)

        new_ks.resize()

        return new_ks


    cdef void resize(self, size_t size=0) except *:
        """
        Change the array capacity.

        :param size_t size: The new capacity size. If not specified or 0, the
            array is shrunk to the last used item. The resulting size
            therefore will always be greater than 0. The only exception
            to this is if the specified size is 0 and no items have been added
            to the array, in which case the array will be effectively shrunk
            to 0.
        """
        if not size:
            size = self._free_i

        tmp = <TripleKey*>PyMem_Realloc(self.data, size * TRP_KLEN)

        if not tmp:
            raise MemoryError('Could not reallocate Keyset data.')

        self.data = tmp
        self.capacity = size
        self.seek()


    cdef Keyset lookup(self, const Key sk, const Key pk, const Key ok):
        """
        Look up triple keys.

        This works in a similar way that the ``Graph`` and ``LmdbStore``
        methods work.

        Any and all the terms may be NULL. A NULL term is treated as unbound.

        :param const Key* sk: s key pointer.
        :param const Key* pk: p key pointer.
        :param const Key* ok: o key pointer.
        """
        cdef:
            TripleKey spok
            Keyset ret = Keyset(self.capacity)
            Key k1, k2
            key_cmp_fn_t cmp_fn

        if sk and pk and ok: # s p o
            pass # TODO

        elif sk:
            k1 = sk
            if pk: # s p ?
                k2 = pk
                cmp_fn = cb.lookup_skpk_cmp_fn

            elif ok: # s ? o
                k2 = ok
                cmp_fn = cb.lookup_skok_cmp_fn

            else: # s ? ?
                cmp_fn = cb.lookup_sk_cmp_fn

        elif pk:
            k1 = pk
            if ok: # ? p o
                k2 = ok
                cmp_fn = cb.lookup_pkok_cmp_fn

            else: # ? p ?
                cmp_fn = cb.lookup_pk_cmp_fn

        elif ok: # ? ? o
            k1 = ok
            cmp_fn = cb.lookup_ok_cmp_fn

        else: # ? ? ?
            return self.copy()

        self.seek()
        while self.get_next(&spok):
            if cmp_fn(&spok, k1, k2):
                ret.add(&spok)

        ret.resize()

        return ret



## Boolean operations.

cdef Keyset merge(Keyset ks1, Keyset ks2):
    """
    Create a Keyset by merging an``ks2`` Keyset with the current one.

    :rtype: Keyset
    """
    cdef:
        TripleKey val
        Keyset ks3 = ks1.copy()

    ks2.seek()
    while ks2.get_next(&val):
        ks3.add(&val, True)

    ks3.resize()

    return ks3


cdef Keyset subtract(Keyset ks1, Keyset ks2):
    """
    Create a Keyset by subtracting an``ks2`` Keyset from the current one.

    :rtype: Keyset
    """
    cdef:
        TripleKey val
        Keyset ks3 = Keyset(ks1.capacity)

    ks1.seek()
    while ks1.get_next(&val):
        if val != NULL_TRP and not ks2.contains(&val):
            ks3.add(&val)

    ks3.resize()

    return ks3


cdef Keyset intersect(Keyset ks1, Keyset ks2):
    """
    Create a Keyset by intersection with an``ks2`` Keyset.

    :rtype: Keyset
    """
    cdef:
        TripleKey val
        Keyset ks3 = Keyset(ks1.capacity)

    ks1.seek()
    while ks1.get_next(&val):
        if val != NULL_TRP and ks2.contains(&val):
            ks3.add(&val)

    ks3.resize()

    return ks3


cdef Keyset xor(Keyset ks1, Keyset ks2):
    """
    Create a Keyset by disjunction (XOR) with an``ks2`` Keyset.

    :rtype: Keyset
    """
    cdef:
        TripleKey val
        Keyset ks3 = Keyset(ks1.capacity + ks2.capacity)

    ks1.seek()
    while ks1.get_next(&val):
        if val != NULL_TRP and not ks2.contains(&val):
            ks3.add(&val)

    ks2.seek()
    while ks2.get_next(&val):
        if val != NULL_TRP and not ks1.contains(&val):
            ks3.add(&val)

    ks3.resize()

    return ks3


