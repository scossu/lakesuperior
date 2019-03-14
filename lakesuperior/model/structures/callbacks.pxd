from lakesuperior.model.base cimport KeyIdx, TripleKey

cdef:
    bint lookup_sk_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    )
    bint lookup_pk_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    )
    bint lookup_ok_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    )
    bint lookup_skpk_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    )
    bint lookup_skok_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    )
    bint lookup_pkok_cmp_fn(
        const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
    )
