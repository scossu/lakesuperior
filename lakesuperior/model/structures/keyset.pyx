import logging

from libc.string cimport memcmp, memcpy
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

cimport lakesuperior.model.structures.callbacks as cb

from lakesuperior.model.base cimport TripleKey, TRP_KLEN


logger = logging.getLogger(__name__)


cdef class Keyset:
    """
    Pre-allocated array (not set, as the name may suggest) of ``TripleKey``s.
    """
    def __cinit__(self, size_t ct=0):
        """
        Initialize and allocate memory for the data set.

        :param size_t ct: Number of elements to be accounted for.
        """
        self.ct = ct
        self.data = <TripleKey*>PyMem_Malloc(self.ct * TRP_KLEN)
        logger.info(f'data address: 0x{<size_t>self.data:02x}')
        if ct and not self.data:
            raise MemoryError('Error allocating Keyset data.')

        self._cur = 0
        self._free_i = 0


    def __dealloc__(self):
        """
        Free the memory.

        This is called when the Python instance is garbage collected, which
        makes it handy to safely pass a Keyset instance across functions.
        """
        #logger.debug(
        #    'Releasing {0} ({1}x{2}) bytes of Keyset @ {3:x}...'.format(
        #        self.size, self.conf.capacity, self.itemsize,
        #        <unsigned long>self.data))
        PyMem_Free(self.data)
        #logger.debug('...done releasing.')


    # Access methods.

    cdef void seek(self, size_t idx=0):
        """
        Place the cursor at a certain index, 0 by default.
        """
        self._cur = idx


    cdef size_t tell(self):
        """
        Tell the position of the cursor in the keyset.
        """
        return self._cur


    cdef bint get_at(self, size_t i, TripleKey* item):
        """
        Get an item at a given index position. Cython-level method.

        :rtype: TripleKey
        """
        if i >= self._free_i:
            return False

        self._cur = i
        item[0] = self.data[i]

        return True


    cdef bint get_next(self, TripleKey* item):
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

        item[0] = self.data[self._cur]
        self._cur += 1

        return True


    cdef void add(self, const TripleKey* val) except *:
        """
        Add a triple key to the array.
        """
        if self._free_i >= self.ct:
            raise MemoryError('No slots left in key set.')

        self.data[self._free_i] = val[0]

        self._free_i += 1


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
        cdef Keyset new_ks = Keyset(self.ct)
        memcpy(new_ks.data, self.data, self.ct * TRP_KLEN)
        new_ks.seek()

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
        self.ct = size
        self.seek()


    cdef Keyset lookup(
            self, const Key* sk, const Key* pk, const Key* ok
    ):
        """
        Look up triple keys.

        This works in a similar way that the ``SimpleGraph`` and ``LmdbStore``
        methods work.

        Any and all the terms may be NULL. A NULL term is treated as unbound.

        :param const Key* sk: s key pointer.
        :param const Key* pk: p key pointer.
        :param const Key* ok: o key pointer.
        """
        cdef:
            TripleKey spok
            Keyset ret = Keyset(self.ct)
            Key* k1 = NULL
            Key* k2 = NULL
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
            if cmp_fn(<TripleKey*>spok, k1, k2):
                ret.add(&spok)

        ret.resize()

        return ret
