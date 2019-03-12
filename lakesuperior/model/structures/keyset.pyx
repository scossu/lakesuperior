from libc.string cimport memcmp
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

from lakesuperior.cy_includes cimport collections as cc
from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)

cdef class BaseKeyset:
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
    def __cinit__(self, size_t ct):
        """
        Initialize and allocate memory for the data set.

        :param size_t ct: Number of elements to be accounted for.
        """
        self.conf.capacity = ct
        self.itemsize = self.get_itemsize() # Set this in concrete classes
        self.size = self.itemsize * self.conf.capacity

        cc.array_conf_init(&self.conf)
        self.conf.capacity = self.conf.capacity
        cc.array_init_conf(&self.data
        if not self.data:
            raise MemoryError()
        self._cur = 0

        #logger.debug('Got malloc sizes: {}, {}'.format(ct, itemsize))
        #logger.debug(
        #    'Allocating {0} ({1}x{2}) bytes of Keyset data...'.format(
        #        self.size, self.conf.capacity, self.itemsize))
        #logger.debug('...done allocating @ {0:x}.'.format(
        #        <unsigned long>self.data))


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

    def iter_init(self):
        """
        Reset the cursor to the initial position.
        """
        self._cur = 0


    def tell(self):
        """
        Tell the position of the cursor in the keyset.
        """
        return self._cur


    cdef unsigned char *get_item(self, i):
        """
        Get an item at a given index position. Cython-level method.

        The item size is known by the ``itemsize`` property of the object.

        :rtype: unsigned char*
        """
        self._cur = i
        return self.data + self.itemsize * i


    cdef bint iter_next(self, unsigned char** val):
        """
        Populate the current value and advance the cursor by 1.

        :param void *val: Addres of value returned. It is NULL if
            the end of the buffer was reached.

        :rtype: bint
        :return: True if a value was found, False if the end of the buffer
            has been reached.
        """
        if self._cur >= self.conf.capacity:
            val = NULL
            return False

        val[0] = self.data + self.itemsize * self._cur
        self._cur += 1

        return True


    cdef bint contains(self, const void *val):
        """
        Whether a value exists in the set.
        """
        cdef unsigned char* stored_val

        self.iter_init()
        while self.iter_next(&stored_val):
            if memcmp(val, stored_val, self.itemsize) == 0:
                return True
        return False


class Keyset(BaseKeyset):
    cdef size_t get_itemsize():
        return KLEN


class DoubleKeyset(BaseKeyset):
    cdef size_t get_itemsize():
        return DBL_KLEN


class TripleKeyset(BaseKeyset):
    cdef size_t get_itemsize():
        return TRP_KLEN
