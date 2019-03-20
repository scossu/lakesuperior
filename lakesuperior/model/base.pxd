cimport lakesuperior.cy_include.cytpl as tpl

ctypedef tpl.tpl_bin Buffer

# NOTE This may change in the future, e.g. if a different key size is to
# be forced.
ctypedef size_t Key

ctypedef Key DoubleKey[2]
ctypedef Key TripleKey[3]
ctypedef Key QuadKey[4]

cdef enum:
    KLEN = sizeof(Key)
    DBL_KLEN = 2 * sizeof(Key)
    TRP_KLEN = 3 * sizeof(Key)
    QUAD_KLEN = 4 * sizeof(Key)

cdef bytes buffer_dump(Buffer* buf)
