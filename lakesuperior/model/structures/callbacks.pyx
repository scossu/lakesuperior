from lakesuperior.model.base cimport KeyIdx, TripleKey

cdef bint lookup_sk_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    ):
    return spok[0] == k1

cdef bint lookup_pk_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    ):
    return spok[1] == k1

cdef bint lookup_ok_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    ):
    return spok[2] == k1

cdef bint lookup_skpk_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    ):
    return spok[0] == k1 and spok[1] == k2

cdef bint lookup_skok_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    ):
    return spok[0] == k1 and spok[2] == k2

cdef bint lookup_pkok_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    ):
    return spok[1] == k1 and spok[2] == k2


