from lakesuperior.cy_include cimport collections as cc
from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)
cdef class Keyset:
    cdef:
        readonly size_t ct, size
        cc.Array* data
        cc.ArrayConf conf

        Keyset lookup(
            self, const KeyIdx* sk, const KeyIdx* pk, const KeyIdx* ok
        )
