from lakesuperior.cy_include cimport cytpl as tpl
from lakesuperior.store.ldp_rs.term cimport Term

ctypedef struct Triple:
    Term s
    Term p
    Term o


cdef:
    int serialize(tuple trp, tpl.tpl_bin *data) except -1
    deserialize(tpl.tpl_bin data)
