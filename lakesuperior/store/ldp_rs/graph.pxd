from lakesuperior.cy_include cimport calg
from lakesuperior.store.ldp_rs.triple cimport Triple
from lakesuperior.store.ldp_rs.lmdb_triplestore cimport LmdbTriplestore

ctypedef struct SetItem:
    unsigned char *data
    size_t size

cdef:
    unsigned int set_item_hash_fn(calg.SetValue data)
    bint set_item_cmp_fn(calg.SetValue v1, calg.SetValue v2)

cdef class SimpleGraph:
    cdef:
        calg.Set *_data
        Triple *_trp # Array of triples that are pointed to by _data.
        LmdbTriplestore store

        void _data_from_lookup(
            self, LmdbTriplestore store, tuple trp_ptn, ctx=*) except *
        _data_as_set(self)

    cpdef void set(self, tuple trp) except *
    cpdef void remove_triples(self, pattern) except *
    cpdef object as_rdflib(self)
    cdef _slice(self, s, p, o)
    cpdef lookup(self, s, p, o)
    cpdef set terms(self, str type)

cdef class Imr(SimpleGraph):
    cdef:
        readonly str uri

    cpdef as_rdflib(self)

