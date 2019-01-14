from rdflib import URIRef, BNode, Literal

#from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint64_t
from libc.stdlib cimport free

from lakesuperior.cy_include cimport cytpl as tpl
from lakesuperior.model.base cimport Buffer


DEF LSUP_TERM_TYPE_URIREF = 1
DEF LSUP_TERM_TYPE_BNODE = 2
DEF LSUP_TERM_TYPE_LITERAL = 3
DEF LSUP_TERM_PK_FMT = b'csss' # Reflects the Term structure
DEF LSUP_TERM_STRUCT_PK_FMT = b'S(' + LSUP_TERM_PK_FMT + b')'


cdef int serialize(const Term *term, Buffer *sterm) except -1:
    """
    Serialize a Term into a binary buffer.

    The returned result is dynamically allocated and must be manually freed.
    """
    cdef:
        unsigned char *addr
        size_t sz

    print('Dump members:')
    print(term[0].type)
    print(term[0].data if term[0].data is not NULL else 'NULL')
    print(term[0].datatype if term[0].datatype is not NULL else 'NULL')
    print(term[0].lang if term[0].lang is not NULL else 'NULL')
    print('Now serializing.')
    tpl.tpl_jot(tpl.TPL_MEM, &addr, &sz, LSUP_TERM_STRUCT_PK_FMT, term)
    print('Serialized.')
    sterm[0].addr = addr
    sterm[0].sz = sz
    print('Assigned to buffer. Returning.')


cdef int deserialize(const Buffer *data, Term *term) except -1:
    """
    Return a term from serialized binary data.
    """
    _pk = tpl.tpl_peek(
            tpl.TPL_MEM | tpl.TPL_DATAPEEK, data[0].addr, data[0].sz,
            LSUP_TERM_PK_FMT, &(term[0].type), &(term[0].data),
            &(term[0].datatype), &(term[0].lang))

    if _pk is NULL:
        raise MemoryError('Error deserializing term.')
    else:
        free(_pk)


cdef int from_rdflib(term_obj, Term *term) except -1:
    """
    Return a Term struct obtained from a Python/RDFLiib term.
    """
    _data = str(term_obj).encode()
    term[0].data = _data

    if isinstance(term_obj, Literal):
        _datatype = (getattr(term_obj, 'datatype') or '').encode()
        _lang = (getattr(term_obj, 'language') or '').encode()
        term[0].type = LSUP_TERM_TYPE_LITERAL
        term[0].datatype = _datatype
        term[0].lang = _lang
    else:
        term[0].datatype = NULL
        term[0].lang = NULL
        if isinstance(term_obj, URIRef):
            term[0].type = LSUP_TERM_TYPE_URIREF
        elif isinstance(term_obj, BNode):
            term[0].type = LSUP_TERM_TYPE_BNODE
        else:
            raise ValueError(f'Unsupported term type: {type(term_obj)}')
    print(f'term data: {term[0].data}')


cdef int serialize_from_rdflib(term_obj, Buffer *data) except -1:
    """
    Return a Buffer struct from a Python/RDFLib term.
    """

    cdef:
        Term _term
        void *addr
        size_t sz

    # From RDFlib
    _data = str(term_obj).encode()
    _term.data = _data

    if isinstance(term_obj, Literal):
        _datatype = (getattr(term_obj, 'datatype') or '').encode()
        _lang = (getattr(term_obj, 'language') or '').encode()
        _term.type = LSUP_TERM_TYPE_LITERAL
        _term.datatype = _datatype
        _term.lang = _lang
    else:
        _term.datatype = NULL
        _term.lang = NULL
        if isinstance(term_obj, URIRef):
            _term.type = LSUP_TERM_TYPE_URIREF
        elif isinstance(term_obj, BNode):
            _term.type = LSUP_TERM_TYPE_BNODE
        else:
            raise ValueError(f'Unsupported term type: {type(term_obj)}')
    #print(f'term data: {_term.data}')

    # # # #

    # Serialize
    print('Dump members:')
    print(_term.type)
    print(_term.data if _term.data is not NULL else 'NULL')
    print(_term.datatype if _term.datatype is not NULL else 'NULL')
    print(_term.lang if _term.lang is not NULL else 'NULL')
    print('Now serializing.')
    tpl.tpl_jot(tpl.TPL_MEM, &addr, &sz, LSUP_TERM_STRUCT_PK_FMT, &_term)
    print('Serialized.')

    print(f'addr: {<unsigned long>addr}; size: {sz}')
    data[0].addr = addr
    data[0].sz = sz

    print('data to be returned: ')
    print((<unsigned char *>data[0].addr)[:data[0].sz])
    #print('Assigned to buffer. Returning.')

    # # # #
    #cdef:
    #    Term _term

    # Resusing other methods. This won't work until I figure out how to
    # not drop the intermediate var in from_rdflib().
    #from_rdflib(term_obj, &_term)
    #print('Dump members in serialize_from_rdflib:')
    #serialize(&_term, data)


cdef object to_rdflib(const Term *term):
    """
    Return an RDFLib term.
    """
    cdef str data = (<bytes>term[0].data).decode()
    if term[0].type == LSUP_TERM_TYPE_LITERAL:
        return Literal(data, datatype=term[0].datatype, lang=term[0].lang)
    else:
        if term[0].type == LSUP_TERM_TYPE_URIREF:
            return URIRef(data)
        elif term[0].type == LSUP_TERM_TYPE_BNODE:
            return BNode(data)
        else:
            raise IOError(f'Unknown term type code: {term[0].type}')


cdef object deserialize_to_rdflib(const Buffer *data):
    """
    Return a Python/RDFLib term from a serialized Cython term.
    """
    cdef Term t

    deserialize(data, &t)

    return to_rdflib(&t)


cdef object to_bytes(const Term *term):
    """
    Return a Python bytes object of the serialized term.
    """
    cdef:
        Buffer pk_t
        unsigned char *bytestream

    serialize(term, &pk_t)
    bytestream = <unsigned char *>pk_t.addr

    return <bytes>(bytestream)[:pk_t.sz]
