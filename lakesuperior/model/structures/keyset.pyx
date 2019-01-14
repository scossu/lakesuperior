from libc.string cimport memcmp
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

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
        self.ct = ct
        self.itemsize = itemsize
        self.size = self.itemsize * self.ct
        self._cur = 0

        #logger.debug('Got malloc sizes: {}, {}'.format(ct, itemsize))
        #logger.debug(
        #    'Allocating {0} ({1}x{2}) bytes of Keyset data...'.format(
        #        self.size, self.ct, self.itemsize))
        self.data = <unsigned char *>PyMem_Malloc(ct * itemsize)
        if not self.data:
            raise MemoryError()
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
        #        self.size, self.ct, self.itemsize,
        #        <unsigned long>self.data))
        PyMem_Free(self.data)
        #logger.debug('...done releasing.')


    cdef void resize(self, size_t ct) except *:
        """
        Resize the result set. Uses ``PyMem_Realloc``.

        Note that resizing to a smaller size does not copy or reallocate the
        data, resizing to a larger size does.

        Also, note that only the number of items can be changed, the item size
        cannot.

        :param size_t ct: Number of items in the result set.
        """
        cdef unsigned char *tmp
        self.ct = ct
        self.size = self.itemsize * self.ct

        #logger.debug(
        #    'Resizing Keyset to {0} ({1}x{2}) bytes @ {3:x}...'.format(
        #        self.itemsize * ct, ct, self.itemsize,
        #        <unsigned long>self.data))
        tmp = <unsigned char *>PyMem_Realloc(self.data, ct * self.itemsize)
        if not tmp:
            raise MemoryError()
        #logger.debug('...done resizing.')

        self.data = tmp


    # Access methods.

    def to_tuple(self):
        """
        Return the data set as a Python tuple.

        :rtype: tuple
        """
        return tuple(
                self.data[i: i + self.itemsize]
                for i in range(0, self.size, self.itemsize))


    def get_item_obj(self, i):
        """
        Get an item at a given index position.

        :rtype: bytes
        """
        if i >= self.ct:
            raise ValueError(f'Index {i} out of range.')

        return self.get_item(i)[: self.itemsize]


    def reset(self):
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


    cdef bint next(self, void *val):
        """
        Populate the current value and advance the cursor by 1.

        :param void *val: Addres of value returned. It is NULL if
            the end of the buffer was reached.

        :rtype: bint
        :return: True if a value was found, False if the end of the buffer
            has been reached.
        """
        if self._cur >= self.ct:
            val = NULL
            return False

        val = self.data + self.itemsize * self._cur
        self._cur += 1

        return True


    cdef bint contains(self, const void *val):
        """
        Whether a value exists in the set.
        """
        cdef void *stored_val

        self.reset()
        while self.next(stored_val):
            if memcmp(val, stored_val, self.itemsize) == 0:
                return True
        return False

