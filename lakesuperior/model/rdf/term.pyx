from uuid import uuid4

from rdflib import URIRef, BNode, Literal

#from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint64_t
from libc.stdlib cimport free
from libc.string cimport memcpy

from lakesuperior.cy_include cimport cytpl as tpl
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.base cimport Buffer, buffer_dump


DEF LSUP_TERM_TYPE_URIREF = 1
DEF LSUP_TERM_TYPE_BNODE = 2
DEF LSUP_TERM_TYPE_LITERAL = 3
DEF LSUP_TERM_PK_FMT = b'csss' # Reflects the Term structure
DEF LSUP_TERM_STRUCT_PK_FMT = b'S(' + LSUP_TERM_PK_FMT + b')'
# URI parsing regular expression. Conforms to RFC3986.
#DEF URI_REGEX_STR = (
#    b'^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?'
#)


__doc__ = """
Term model.

``Term`` is not defined as a Cython or Python class. It is a C structure,
hence only visible by the Cython layer of the application.

Terms can be converted from/to RDFlib terms, and deserialized from, or
serialized to, binary buffer structures. This is the form that terms are stored
in the data store.

If uses require a public API, a proper Term Cython class with a Python API
could be developed in the future.

"""


#cdef char* ptn = URI_REGEX_STR
#regcomp(&uri_regex, ptn, REG_NOSUB)
# Compile with no catch groups.
# TODO This should be properly cleaned up on application shutdown:
# regfree(&uri_regex)

#cdef int term_new(
#    Term* term, char type, char* data, char* datatype=NULL, char* lang=NULL
#) except -1:
#    if regexec(&uri_regex, data, 0, NULL, 0) == REG_NOMATCH:
#        raise ValueError('Not a valid URI.')
#    term.type = type
#    term.data = (
#        data # TODO use C UUID v4 (RFC 4122) generator
#        if term.type == LSUP_TERM_TYPE_BNODE
#        else data
#    )
#    if term.type == LSUP_TERM_TYPE_LITERAL:
#        term.datatype = datatype
#        term.lang = lang
#
#    return 0


cdef int serialize(const Term *term, Buffer *sterm) except -1:
    """
    Serialize a Term into a binary buffer.
    """
    tpl.tpl_jot(
        tpl.TPL_MEM, &sterm.addr, &sterm.sz, LSUP_TERM_STRUCT_PK_FMT, term
    )


cdef int deserialize(const Buffer *data, Term *term) except -1:
    """
    Return a term from serialized binary data.
    """
    #print(f'Deserializing: {buffer_dump(data)}')
    _pk = tpl.tpl_peek(
            tpl.TPL_MEM | tpl.TPL_DATAPEEK, data.addr, data.sz,
            LSUP_TERM_PK_FMT, &(term.type), &(term.data),
            &(term.datatype), &(term.lang))

    if _pk is NULL:
        raise MemoryError('Error deserializing term.')
    else:
        free(_pk)


cdef int from_rdflib(term_obj, Term *term) except -1:
    """
    Return a Term struct obtained from a Python/RDFLib term.
    """
    _data = str(term_obj).encode()
    term.data = _data
    term.datatype = NULL
    term.lang = NULL

    if isinstance(term_obj, Literal):
        _datatype = getattr(term_obj, 'datatype', None)
        _lang = getattr(term_obj, 'language', None)
        term.type = LSUP_TERM_TYPE_LITERAL
        if _datatype:
            _datatype = _datatype.encode()
            term.datatype = _datatype
        if _lang:
            _lang = _lang.encode()
            term.lang = _lang
    else:
        if isinstance(term_obj, URIRef):
            term.type = LSUP_TERM_TYPE_URIREF
        elif isinstance(term_obj, BNode):
            term.type = LSUP_TERM_TYPE_BNODE
        else:
            raise ValueError(f'Unsupported term type: {type(term_obj)}')


cdef int serialize_from_rdflib(term_obj, Buffer *data) except -1:
    """
    Return a Buffer struct from a Python/RDFLib term.
    """

    cdef:
        Term _term
        void *addr
        size_t sz

    from_rdflib(term_obj, &_term)
    serialize(&_term, data)


cdef object to_rdflib(const Term *term):
    """
    Return an RDFLib term.
    """
    cdef str data = (<bytes>term.data).decode()
    if term.type == LSUP_TERM_TYPE_LITERAL:
        if term.lang:
            params = {'lang': (<bytes>term.lang).decode()}
        elif term.datatype:
            params = {'datatype': (<bytes>term.datatype).decode()}
        else:
            params = {}
        return Literal(data, **params)
    else:
        if term.type == LSUP_TERM_TYPE_URIREF:
            return URIRef(data)
        elif term.type == LSUP_TERM_TYPE_BNODE:
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
