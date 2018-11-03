from rdflib import URIRef, BNode, Literal

#from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc, free

from lakesuperior.cy_include cimport cytpl as tpl


DEF LSUP_TERM_TYPE_URIREF = 1
DEF LSUP_TERM_TYPE_BNODE = 2
DEF LSUP_TERM_TYPE_LITERAL = 3
DEF LSUP_PK_FMT_ID = b'S(cs)'
DEF LSUP_PK_FMT_LIT = b'S(csss)'


cdef int serialize(
        term, unsigned char **pack_data, size_t *pack_size) except -1:
    cdef:
        bytes term_data = term.encode()
        bytes term_datatype
        bytes term_lang
        IdentifierTerm id_t
        LiteralTerm lit_t

    if isinstance(term, Literal):
        term_datatype = (getattr(term, 'datatype') or '').encode()
        term_lang = (getattr(term, 'language') or '').encode()

        lit_t.type = LSUP_TERM_TYPE_LITERAL
        lit_t.data = term_data
        lit_t.datatype = <unsigned char *>term_datatype
        lit_t.lang = <unsigned char *>term_lang

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


cdef deserialize(const unsigned char *data, const size_t data_size):
    cdef:
        char term_type
        char *fmt = NULL
        char *_pk = NULL
        unsigned char *term_data = NULL
        unsigned char *term_lang = NULL
        unsigned char *term_datatype = NULL

    datatype = None
    lang = None

    fmt = tpl.tpl_peek(tpl.TPL_MEM, data, data_size)
    try:
        if fmt == LSUP_PK_FMT_LIT:
            _pk = tpl.tpl_peek(
                    tpl.TPL_MEM | tpl.TPL_DATAPEEK, data, data_size, b'csss',
                    &term_type, &term_data, &term_datatype, &term_lang)
            if len(term_datatype) > 0:
                datatype = term_datatype.decode()
            elif len(term_lang) > 0:
                lang = term_lang.decode()

            return Literal(term_data.decode(), datatype=datatype, lang=lang)

        elif fmt == LSUP_PK_FMT_ID:
            _pk = tpl.tpl_peek(
                    tpl.TPL_MEM | tpl.TPL_DATAPEEK, data, data_size, b'cs',
                    &term_type, &term_data)
            uri = term_data.decode()
            if term_type == LSUP_TERM_TYPE_URIREF:
                return URIRef(uri)
            elif term_type == LSUP_TERM_TYPE_BNODE:
                return BNode(uri)
            else:
                raise IOError(f'Unknown term type code: {term_type}')
        else:
            msg = f'Unknown structure pack format: {fmt}'
            raise IOError(msg)
    finally:
        free(term_data)
        free(term_datatype)
        free(term_lang)
        free(_pk)
        free(fmt)
