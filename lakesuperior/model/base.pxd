cimport lakesuperior.cy_include.cytpl as tpl

ctypedef tpl.tpl_bin Buffer

# NOTE This may change in the future, e.g. if a different key size is to
# be forced.
ctypedef size_t KeyIdx

ctypedef KeyIdx Key[1]
ctypedef KeyIdx DoubleKey[2]
ctypedef KeyIdx TripleKey[3]
ctypedef KeyIdx QuadKey[4]

cdef enum:
    KLEN = sizeof(KeyIdx)
    DBL_KLEN = 2 * sizeof(KeyIdx)
    TRP_KLEN = 3 * sizeof(KeyIdx)
    QUAD_KLEN = 4 * sizeof(KeyIdx)

cdef bytes buffer_dump(Buffer* buf)
