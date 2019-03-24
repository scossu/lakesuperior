from lakesuperior.model.base cimport Key, TripleKey

cdef:
    bint lookup_sk_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )

    bint lookup_pk_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )

    bint lookup_ok_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )

    bint lookup_skpk_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )

    bint lookup_skok_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )

    bint lookup_pkok_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )

    bint lookup_none_cmp_fn(
        const TripleKey* spok, const Key k1, const Key k2
    )
