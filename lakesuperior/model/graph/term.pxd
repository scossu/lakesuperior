from cymem.cymem cimport Pool

from lakesuperior.model.base cimport Buffer

ctypedef struct Term:
    char type
    char *data
    char *datatype
    char *lang

cdef:
    # Temporary TPL variable.
    char *_pk

    int serialize(const Term *term, Buffer *sterm, Pool pool=*) except -1
    int deserialize(const Buffer *data, Term *term) except -1
    int from_rdflib(term_obj, Term *term) except -1
    int serialize_from_rdflib(term_obj, Buffer *data, Pool pool=*) except -1
    object deserialize_to_rdflib(const Buffer *data)
    object to_rdflib(const Term *term)
    object to_bytes(const Term *term)