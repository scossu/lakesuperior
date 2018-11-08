from lakesuperior.store.ldp_rs cimport term

cdef int serialize(tuple trp, tpl.tpl_bin *data) except -1:
    """
    Serialize a triple expressed as a tuple of RDFlib terms.

    :param tuple trp: 3-tuple of RDFlib terms.

    :rtype: Triple
    """
    cdef:
        Triple strp
        tpl.tpl_bin s, p, o

    strp.s = s
    strp.p = p
    strp.o = o

    term.serialize(trp[0], &s.addr, &s.sz)
    term.serialize(trp[1], &p.addr, &p.sz)
    term.serialize(trp[2], &o.addr, &o.sz)

    return strp


cdef tuple deserialize(Triple strp):
    """
    Deserialize a ``Triple`` structure into a tuple of terms.

    :rtype: tuple
    """
    s = term.deserialize(strp.s.addr, strp.s.sz)
    p = term.deserialize(strp.p.addr, strp.p.sz)
    o = term.deserialize(strp.o.addr, strp.o.sz)

    return s, p, o


