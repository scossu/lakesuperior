from lakesuperior.cy_include cimport cytpl as tpl

cdef class Term:
    char type
    char *data
    char *datatype
    char *lang

    # Temporary vars that get cleaned up on object deallocation.
    char *_fmt
    char *_pk

    tpl.tpl_bin serialize(self)
    object to_python()

    Term from_buffer(const unsigned char *data, const size_t size)

