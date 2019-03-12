from lakesuperior.cy_includes cimport collections as cc
from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)
cdef class BaseKeyset:
    cdef:
        readonly cc.Array data
        readonly size_t ct, size
        size_t _cur
        cc.ArrayConf conf

        void resize(self, size_t ct) except *
        unsigned char *get_item(self, i)
        bint iter_next(self, unsigned char** val)
        bint contains(self, const void *val)


cdef class Keyset(BaseKeyset):
    cdef size_t get_itemsize()


cdef class DoubleKeyset(BaseKeyset):
    cdef size_t get_itemsize()


cdef class TripleKeyset(BaseKeyset):
    cdef size_t get_itemsize()
