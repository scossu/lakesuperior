# cython: language_level = 3
# cython: boundschecking = False
# cython: wraparound = False
# cython: profile = True

from rdflib import URIRef, BNode, Literal

#from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdlib cimport malloc, free
#from libc.string cimport memcpy

from lakesuperior.cy_include cimport cytpl as tpl

DEF LSUP_TERM_TYPE_URIREF = 1
DEF LSUP_TERM_TYPE_BNODE = 2
DEF LSUP_TERM_TYPE_LITERAL = 3
DEF LSUP_PK_FMT_ID = b'S(cs)'
DEF LSUP_PK_FMT_LIT = b'S(csss)'


cdef int serialize(
        term, unsigned char **pack_data, size_t *pack_size) except -1:
    term_data = term.encode()

    if isinstance(term, Literal):
        term_datatype = (getattr(term, 'datatype') or '').encode()
        term_lang = (getattr(term, 'language') or '').encode()

        lit_t.type = LSUP_TERM_TYPE_LITERAL
        lit_t.data = term_data
        lit_t.datatype = term_datatype
        lit_t.lang = term_lang

        tpl.tpl_jot(tpl.TPL_MEM, pack_data, pack_size, LSUP_PK_FMT_LIT, &lit_t)
    else:
        if isinstance(term, URIRef):
            id_t.type = LSUP_TERM_TYPE_URIREF
        elif isinstance(term, BNode):
            id_t.type = LSUP_TERM_TYPE_BNODE
        else:
            raise ValueError(f'Unsupported term type: {type(term)}')
        id_t.data = term_data
        tpl.tpl_jot(tpl.TPL_MEM, pack_data, pack_size, LSUP_PK_FMT_ID, &id_t)


cdef deserialize(unsigned char *data, size_t data_size):
    cdef char *fmt

    fmt = tpl.tpl_peek(tpl.TPL_MEM, data, data_size)
    try:
        if fmt == LSUP_PK_FMT_LIT:
            fmt = tpl.tpl_peek(
                    tpl.TPL_MEM | tpl.TPL_DATAPEEK, data, data_size,
                    <unsigned char *>'csss',
                    &term_type, &term_data, &term_datatype, &term_lang)
            datatype = lang = None
            if len(term_datatype) > 0:
                datatype = term_datatype.decode()
            elif len(term_lang) > 0:
                lang = term_lang.decode()

            return Literal(term_data.decode(), datatype=datatype, lang=lang)

        elif fmt == LSUP_PK_FMT_ID:
            fmt = tpl.tpl_peek(
                    tpl.TPL_MEM | tpl.TPL_DATAPEEK, data, data_size,
                    <unsigned char *>'cs',
                    &term_type, &term_data)
            if term_type == LSUP_TERM_TYPE_URIREF:
                return URIRef(term_data.decode())
            elif term_type == LSUP_TERM_TYPE_BNODE:
                return BNode(term_data.decode())
            else:
                raise IOError(f'Unknown term type code: {term_type}')
        else:
            raise IOError(f'Unknown structure pack format: {fmt}')
    finally:
        free(fmt)

