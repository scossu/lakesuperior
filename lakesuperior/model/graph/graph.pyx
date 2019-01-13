from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

from lakesuperior.util.hash cimport HLEN_32, Hash32, hash32

ctypedef void *SetItem
ctypedef struct Index:
    size_t *addr
    size_t ct

cdef class VarSet:
    """
    Variable-size set of variable-size values.
    """
    cdef:
        # Data blob. Stored contibuously in memory, and found by index.
        void *_data
        # Total size of data.
        size_t _data_sz
        # Index used to find start and end of each item.
        Index _index
        # KeySet of hashes of the set items.
        Keyset _hashes

    def __cinit__(self):
        self._data = PyMem_Malloc(0)
        self._hashes = Keyset(0, sizeof(Hash32))
        self._data_sz = 0


    def __dealloc__(self):
        PyMem_Free(self._data)


    cdef int add(self, const SetItem data, Index *idx) except -1:
        """
        Add a number of items.

        The items' content as a blob and their end boundaries must be given
        as an array of ``size_t``.
        """"
        #cdef size_t grow_sz = idx.addr[idx.ct - 1]
        # Last index indicates the position of the last byte
        cdef:
            size_t i, cur = 0, data_exp_sz, hash_exp_sz
            void *_tmp_data
            Hash32 hash
            Buffer msg
            SetItem *item

        # Resize data sets to maximium possible size for this function call.
        _tmp_data = PyMem_Realloc(self._data, idx.addr[idx.ct - 1])
        if not _tmp_data:
            raise MemoryError('Unable to allocate memory for set data.')
        self._hashes.resize(self._hashes.ct + idx.ct)

        for i in idx.ct:
            # Iterate over the items in the index and verify if they can be
            # added if they are not duplicates.
            msg.addr = data + cur
            msg.sz = idx[i] - cur
            hash32(&msg, &hash)

            if not self.hashes.contains(hash):
                # Add to the data.
                memcpy(_tmp_data + i * HLEN_32, msg.addr, msg.sz)
                # Add to the hashes keyset.
                memcpy(self._hashes + self._data_sz, hash, HLEN32)
                # Record the memory expansion.
                self._data_sz += msg.sz

            cur = idx[i]

        # Shrink data back to their actual size.
        self.hashes.resize(cur)
        _tmp_data = PyMem_Malloc(self._data_sz)
        if not _tmp_data :
            raise MemoryError('Unable to allocate memory for set data.')
        self._data = _tmp_data

