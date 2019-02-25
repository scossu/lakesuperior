cdef class Keyset:
    cdef:
        readonly unsigned char *data
        readonly unsigned char itemsize
        readonly size_t ct, size
        size_t _cur

        void resize(self, size_t ct) except *
        unsigned char *get_item(self, i)
        bint iter_next(self, unsigned char** val)
        bint contains(self, const void *val)

