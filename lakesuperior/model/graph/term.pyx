from rdflib import URIRef, BNode, Literal

#from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint64_t
from libc.stdlib cimport free
from libc.string cimport memcpy

from cymem.cymem cimport Pool

from lakesuperior.cy_include cimport cytpl as tpl
from lakesuperior.model.base cimport Buffer


DEF LSUP_TERM_TYPE_URIREF = 1
DEF LSUP_TERM_TYPE_BNODE = 2
DEF LSUP_TERM_TYPE_LITERAL = 3
DEF LSUP_TERM_PK_FMT = b'csss' # Reflects the Term structure
DEF LSUP_TERM_STRUCT_PK_FMT = b'S(' + LSUP_TERM_PK_FMT + b')'


cdef int serialize(const Term *term, Buffer *sterm, Pool pool=None) except -1:
    """
    Serialize a Term into a binary buffer.

    The returned result is dynamically allocated and must be manually freed.
    """
    cdef:
        unsigned char *addr
        size_t sz

    tpl.tpl_jot(tpl.TPL_MEM, &addr, &sz, LSUP_TERM_STRUCT_PK_FMT, term)
    if pool is None:
        sterm.addr = addr
    else:
        # addr is within this function scope. Must be copied to the cymem pool.
        sterm.addr = pool.alloc(sz, 1)
        if not sterm.addr:
            raise MemoryError()
        memcpy(sterm.addr, addr, sz)
    sterm.sz = sz


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


cdef int serialize_from_rdflib(
        term_obj, Buffer *data, Pool pool=None
    ) except -1:
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

    tpl.tpl_jot(tpl.TPL_MEM, &addr, &sz, LSUP_TERM_STRUCT_PK_FMT, &_term)

    data[0].addr = addr
    data[0].sz = sz


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