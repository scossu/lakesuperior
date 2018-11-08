from rdflib import URIRef, BNode, Literal

#from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc, free

from lakesuperior.cy_include cimport cytpl as tpl


DEF LSUP_TERM_TYPE_URIREF = 1
DEF LSUP_TERM_TYPE_BNODE = 2
DEF LSUP_TERM_TYPE_LITERAL = 3
DEF LSUP_TERM_PK_FMT = b'csss'
DEF LSUP_TERM_STRUCT_PK_FMT = b'S(' + LSUP_TERM_PK_FMT + b')'


cdef class Term:
    """
    RDF term: URI reference, blank node or literal.
    """
    def __cinit__(self, const tpl.tpl_bin data):
        """
        Initialize a Term from pack data.

        :param tpl.tpl_bin data: a TPL binary buffer packed according to the
            term structure format.
        """
        self._pk = tpl.tpl_peek(
                tpl.TPL_MEM | tpl.TPL_DATAPEEK, data.addr, data.sz,
                LSUP_TERM_PK_FMT, &self.term_type, &self.data, &self.datatype,
                &self.lang)


    def __dealloc__(self):
        free(self.data)
        free(self.datatype)
        free(self.lang)
        free(self._pk)
        free(self._fmt)


    def to_py_term(self):
        """
        Return an RDFLib term.
        """
        data = (<bytes>self.data).decode()
        if self.term_type == LSUP_TERM_TYPE_LITERAL:
            return Literal(
                data, datatype=datatype, lang=lang)
        else:
            uri = term_data.decode()
            if self.term_type == LSUP_TERM_TYPE_URIREF:
                return URIRef(uri)
            elif self.term_type == LSUP_TERM_TYPE_BNODE:
                return BNode(uri)
            else:
                raise IOError(f'Unknown term type code: {self.term_type}')


    def to_bytes(self):
        """
        Return a Python bytes object of the serialized term.
        """
        ser_data = self.serialize()
        return <bytes>ser_data.data[:ser_data.sz]


    cdef tpl.tpl_bin serialize(self):
            #term_obj, unsigned char **pack_data, size_t *pack_size) except -1:
        cdef:
            bytes term_data = term_obj.encode()
            bytes term_datatype
            bytes term_lang
            term_obj term

        if isinstance(term_obj, Literal):
            term_datatype = (getattr(term_obj, 'datatype') or '').encode()
            term_lang = (getattr(term_obj, 'language') or '').encode()

            term.type = LSUP_TERM_TYPE_LITERAL
            term.data = term_data
            term.datatype = <unsigned char *>term_datatype
            term.lang = <unsigned char *>term_lang
        else:
            if isinstance(term_obj, URIRef):
                term.type = LSUP_TERM_TYPE_URIREF
            elif isinstance(term_obj, BNode):
                term.type = LSUP_TERM_TYPE_BNODE
            else:
                raise ValueError(f'Unsupported term type: {type(term_obj)}')
            term.data = term_data

        tpl.tpl_jot(
            tpl.TPL_MEM, pack_data, pack_size, LSUP_TERM_STRUCT_PK_FMT, &term)
