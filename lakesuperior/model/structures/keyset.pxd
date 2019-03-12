from lakesuperior.cy_includes cimport collections as cc
from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)
cdef class BaseKeyset:
    cdef:
        readonly size_t ct, size
        readonly cc.Array* data
        readonly cc.ArrayConf conf

        size_t get_itemsize(self)
        unsigned char *get_item(self, i)
        bint iter_next(self, unsigned char** val)
        bint contains(self, const void *val)


cdef class Keyset(BaseKeyset):


cdef class DoubleKeyset(BaseKeyset):


cdef class TripleKeyset(BaseKeyset):
