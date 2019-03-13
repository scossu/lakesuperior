from lakesuperior.cy_include cimport collections as cc
from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)
cdef class Keyset:
    cdef:
        readonly size_t ct, size
        readonly cc.Array* data
        readonly cc.ArrayConf conf

        unsigned char *get_item(self, i)
        bint iter_next(self, unsigned char** val)
        bint contains(self, const void *val)

        Keyset lookup(
            self, const KeyIdx* sk, const KeyIdx* pk, const KeyIdx* ok
        )
