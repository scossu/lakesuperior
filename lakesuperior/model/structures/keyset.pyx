from libc.string cimport memcmp
from libc.stdlib cimport free

cimport lakesuperior.cy_include.collections as cc
cimport lakesuperior.model.structures.callbacks as cb

from lakesuperior.model.base cimport (
    TRP_KLEN, KeyIdx, Key, DoubleKey, TripleKey, Buffer
)

cdef class Keyset:
    """
    Pre-allocated result set.
    """
    def __cinit__(self, size_t ct=1):
        """
        Initialize and allocate memory for the data set.

        :param size_t ct: Number of elements to be accounted for.
        """
        cc.array_conf_init(&self.conf)
        self.conf.capacity = ct or 1
        self.conf.exp_factor = .5

        cc.array_new_conf(&self.conf, &self.data)
        if not self.data:
            raise MemoryError()


    def __dealloc__(self):
        """
        Free the memory.

        This is called when the Python instance is garbage collected, which
        makes it handy to safely pass a Keyset instance across functions.
        """
        if self.data:
            free(self.data)


    # Access methods.

    cdef Keyset lookup(
            self, const KeyIdx* sk, const KeyIdx* pk, const KeyIdx* ok
    ):
        """
        Look up triple keys.

        This works in a similar way that the ``SimpleGraph`` and ``LmdbStore``
        methods work.

        Any and all the terms may be NULL. A NULL term is treated as unbound.

        :param const KeyIdx* sk: s key pointer.
        :param const KeyIdx* pk: p key pointer.
        :param const KeyIdx* ok: o key pointer.
        """
        cdef:
            void* cur
            cc.ArrayIter it
            TripleKey spok
            Keyset ret
            KeyIdx* k1 = NULL
            KeyIdx* k2 = NULL
            key_cmp_fn_t cmp_fn

        cc.array_iter_init(&it, self.data)

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
            return self # TODO Placeholder. This should actually return a copy.

        ret = Keyset(256) # TODO Totally arbitrary.
        while cc.array_iter_next(&it, &cur) != cc.CC_ITER_END:
            if cmp_fn(<TripleKey*>spok, k1, k2):
                if cc.array_add(ret.data, spok) != cc.CC_OK:
                    raise RuntimeError('Error adding triple key.')

        return ret
