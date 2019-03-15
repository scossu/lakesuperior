cimport lakesuperior.cy_include.collections as cc

from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)

ctypedef bint (*key_cmp_fn_t)(
    const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
)

cdef class Keyset:
    cdef:
        readonly size_t ct, size
        cc.Array* data
        cc.ArrayConf conf

        Keyset lookup(
            self, const KeyIdx* sk, const KeyIdx* pk, const KeyIdx* ok
        )
