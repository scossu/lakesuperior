from lakesuperior.model.base cimport (
    KeyIdx, Key, DoubleKey, TripleKey, Buffer
)

ctypedef bint (*key_cmp_fn_t)(
    const TripleKey* spok, const KeyIdx* k1, const KeyIdx* k2
)

cdef class Keyset:
    cdef:
        TripleKey* data
        size_t ct
        size_t _cur # Index cursor used to look up values.
        size_t _free_i # Index of next free slot.

        void seek(self, size_t idx=*)
        size_t tell(self)
        bint get_at(self, size_t i, TripleKey* item)
        bint get_next(self, TripleKey* item)
        void add(self, const TripleKey* val) except *
        bint contains(self, const TripleKey* val)
        Keyset copy(self)
        void resize(self, size_t size=*) except *
        Keyset lookup(
            self, const KeyIdx* sk, const KeyIdx* pk, const KeyIdx* ok
        )
