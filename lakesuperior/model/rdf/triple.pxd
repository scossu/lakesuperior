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
