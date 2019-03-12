from lakesuperior.cy_include cimport cytpl as tpl

ctypedef tpl.tpl_bin Buffer

# NOTE This may change in the future, e.g. if a different key size is to
# be forced.
ctypedef size_t KeyIdx

ctypedef KeyIdx Key[1]
ctypedef KeyIdx DoubleKey[2]
ctypedef KeyIdx TripleKey[3]
ctypedef KeyIdx QuadKey[4]

cdef enum:
    KLEN = sizeof(Key)
    DBL_KLEN = sizeof(DoubleKey)
    TRP_KLEN = sizeof(TripleKey)
    QUAD_KLEN = sizeof(QuadKey)

cdef bytes buffer_dump(Buffer* buf)
