from lakesuperior.cy_include cimport cytpl as tpl

ctypedef tpl.tpl_bin Buffer
ctypedef struct Term:
    char type
    char *data
    char *datatype
    char *lang

cdef:
    # Temporary TPL variable.
    char *_pk

    int serialize(const Term *term, tpl.tpl_bin *sterm) except -1
    int deserialize(const Buffer *data, Term *term) except -1
    int from_rdflib(term_obj, Term *term) except -1
    Buffer *serialize_from_rdflib(term_obj) except NULL
    object deserialize_to_rdflib(const Buffer *data)
    object to_rdflib(const Term *term)
    object to_bytes(const Term *term)
