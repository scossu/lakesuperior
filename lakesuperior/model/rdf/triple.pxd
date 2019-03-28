#from lakesuperior.cy_include cimport cytpl as tpl
from lakesuperior.model.base cimport Buffer
from lakesuperior.model.rdf.term cimport Term

# Triple of Term structs.
ctypedef struct Triple:
    Term *s
    Term *p
    Term *o

# Triple of serialized terms.
ctypedef struct BufferTriple:
    Buffer *s
    Buffer *p
    Buffer *o

#cdef:
#    int serialize(tuple trp, tpl.tpl_bin *data) except -1
#    tuple deserialize(tpl.tpl_bin data)
