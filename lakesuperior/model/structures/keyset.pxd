from lakesuperior.model.base cimport (
    Key, Key, DoubleKey, TripleKey, Buffer
)

ctypedef bint (*key_cmp_fn_t)(
    const TripleKey* spok, const Key k1, const Key k2
)

cdef class Keyset:
    cdef:
        TripleKey* data
        size_t capacity
        size_t _cur # Index cursor used to look up values.
        size_t _free_i # Index of next free slot.
        float expand_ratio # By how much storage is automatically expanded when
                           # full. 1 means the size doubles, 0.5 a 50%
                           # increase. 0 means that storage won't be
                           # automatically expanded and adding above capacity
                           # will raise an error.

        void seek(self, size_t idx=*)
        size_t size(self)
        size_t tell(self)
        bint get_next(self, TripleKey* item)
        void add(self, const TripleKey* val, bint check_dup=*) except *
        void remove(self, const TripleKey* val) except *
        bint contains(self, const TripleKey* val)
        Keyset copy(self)
        Keyset sparse_copy(self)
        void resize(self, size_t size=*) except *
        Keyset lookup(self, const Key sk, const Key pk, const Key ok)

cdef:
    Keyset merge(Keyset ks1, Keyset ks2)
    Keyset subtract(Keyset ks1, Keyset ks2)
    Keyset intersect(Keyset ks1, Keyset ks2)
    Keyset xor(Keyset ks1, Keyset ks2)
