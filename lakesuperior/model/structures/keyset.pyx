from libc.string cimport memcmp
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

from lakesuperior.cy_includes cimport collections as cc
from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)

cdef class BaseKeyset:
    """
    Pre-allocated result set.
    """
    def __cinit__(self, size_t ct):
        """
        Initialize and allocate memory for the data set.

        :param size_t ct: Number of elements to be accounted for.
        """
        self.itemsize = self.get_itemsize() # Set this in concrete classes

        cc.array_conf_init(&self.conf)
        self.conf.capacity = ct
        self.conf.exp_factor = .5

        cc.array_init_conf(&self.conf, &self.data)
        if not self.data:
            raise MemoryError()


    def __dealloc__(self):
        """
        Free the memory.

        This is called when the Python instance is garbage collected, which
        makes it handy to safely pass a Keyset instance across functions.
        """
        PyMem_Free(self.data)


    # Access methods.

    cdef size_t get_itemsize(self):
        raise NotImplementedError()


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

    cdef TripleKeyset lookup(
            self, const KeyIdx* sk, const KeyIdx* pk, const KeyIdx* ok
    ):
        """
        Look up triple keys in a similar way that the ``SimpleGraph`` and
        ``LmdbStore`` methods work.

        Any and all the terms may be NULL. A NULL term is treated as unbound.

        :param const KeyIdx* sk: s key pointer.
        :param const KeyIdx* pk: p key pointer.
        :param const KeyIdx* ok: o key pointer.
        """
        cdef:
            void* cur
            cc.ArrayIter it
            TripleKey spok
            TripleKeyset ret
            KeyIdx bk1 = NULL, bk2 = NULL

        cc.array_iter_init(&it, self.data)

        if sk and pk and ok: # s p o
            pass # TODO

        elif sk:
            bt1 = sk[0]
            if pk: # s p ?
                bt2 = pk[0]
                cmp_fn = cb.lookup_skpk_cmp_fn

            elif ok: # s ? o
                bt2 = ok[0]
                cmp_fn = cb.lookup_skok_cmp_fn

            else: # s ? ?
                cmp_fn = cb.lookup_sk_cmp_fn

        elif pk:
            bt1 = pk[0]
            if ok: # ? p o
                bt2 = ok[0]
                cmp_fn = cb.lookup_pkok_cmp_fn

            else: # ? p ?
                cmp_fn = cb.lookup_pk_cmp_fn

        elif ok: # ? ? o
            bt1 = ok[0]
            cmp_fn = cb.lookup_ok_cmp_fn

        else: # ? ? ?
            return self # TODO Placeholder. This should actually return a copy.

        ret = TripleKeyset(256) # TODO Totally arbitrary.
        while cc.array_iter_next(&it, &cur) != cc.CC_ITER_END:
            if cmp_fn(<TripleKey*>spok, t1, t2):
                if cc.array_add(ret.data, spok) != cc.CC_OK:
                    raise RuntimeError('Error adding triple key.')

        return ret
