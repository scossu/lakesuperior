import logging

from libc.string cimport memcmp, memcpy
from cpython.mem cimport PyMem_Malloc, PyMem_Free

from lakesuperior.model.base cimport TripleKey, TRP_KLEN


logger = logging.getLogger(__name__)


cdef class Keyset:
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
        logger.info('Adding triple to key set.')
        logger.info(f'triple: {val[0][0]} {val[0][1]} {val[0][2]}')
        logger.info(f'_free_i: {self._free_i}')

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
