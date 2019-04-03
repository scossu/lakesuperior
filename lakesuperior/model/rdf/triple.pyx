#from lakesuperior.store.ldp_rs cimport term

__doc__ = """
Triple model.

This is a very light-weight implementation of a Triple model, available as
C structures only. Two types of structures are defined: ``Triple``, with
pointers to :py:model:`lakesuperior.model.rdf.term` objects, and
``BufferTriple``, with pointers to byte buffers of serialized terms.

"""


#cdef int serialize(tuple trp, tpl.tpl_bin *data) except -1:
#    """
#    Serialize a triple expressed as a tuple of RDFlib terms.
#
#    :param tuple trp: 3-tuple of RDFlib terms.
#
#    :rtype: Triple
#    """
#    cdef:
#        Triple strp
#        Term *s
#        Term *p
#        Term *o
#
#    strp.s = s
#    strp.p = p
#    strp.o = o
#
##    term.serialize(s)
##    term.serialize(p)
##    term.serialize(o)
#
#    return strp
#
#
#cdef tuple deserialize(Triple strp):
#    """
#    Deserialize a ``Triple`` structure into a tuple of terms.
#
#    :rtype: tuple
#    """
#    pass
##    s = term.deserialize(strp.s.addr, strp.s.sz)
##    p = term.deserialize(strp.p.addr, strp.p.sz)
##    o = term.deserialize(strp.o.addr, strp.o.sz)
##
##    return s, p, o
#
#
