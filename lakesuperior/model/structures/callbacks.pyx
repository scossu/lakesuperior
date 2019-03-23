from lakesuperior.model.base cimport Key, TripleKey

cdef inline bint lookup_sk_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """ Keyset lookup for S key. """
    return spok[0][0] == k1


cdef inline bint lookup_pk_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """ Keyset lookup for P key. """
    return spok[0][1] == k1


cdef inline bint lookup_ok_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """ Keyset lookup for O key. """
    return spok[0][2] == k1


cdef inline bint lookup_skpk_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """ Keyset lookup for S and P keys. """
    return spok[0][0] == k1 and spok[0][1] == k2


cdef inline bint lookup_skok_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """ Keyset lookup for S and O keys. """
    return spok[0][0] == k1 and spok[0][2] == k2


cdef inline bint lookup_pkok_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """ Keyset lookup for P and O keys. """
    return spok[0][1] == k1 and spok[0][2] == k2


cdef inline bint lookup_none_cmp_fn(
    const TripleKey* spok, const Key k1, const Key k2
):
    """
    Dummy callback for queries with all parameters unbound.

    This function always returns ``True`` 
    """
    return True

