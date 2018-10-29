cdef class Keyset:
    cdef:
        readonly unsigned char *data
        readonly unsigned char itemsize
        readonly size_t ct, size

        void resize(self, size_t ct) except *
        unsigned char *get_item(self, i)

