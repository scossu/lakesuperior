cimport lakesuperior.cy_include.cytpl as tpl

ctypedef tpl.tpl_bin Buffer

# NOTE This may change in the future, e.g. if a different key size is to
# be forced.
ctypedef size_t Key

ctypedef Key DoubleKey[2]
ctypedef Key TripleKey[3]
ctypedef Key QuadKey[4]

cdef enum:
    KLEN = sizeof(Key)
    DBL_KLEN = 2 * sizeof(Key)
    TRP_KLEN = 3 * sizeof(Key)
    QUAD_KLEN = 4 * sizeof(Key)

cdef bytes buffer_dump(Buffer* buf)

# "NULL" key, a value that is never user-provided. Used to mark special
# values (e.g. deleted records).
cdef Key NULL_KEY = 0

# Value of first key inserted in an empty term database.
cdef Key FIRST_KEY = 1

# "NULL" triple, a value that is never user-provided. Used to mark special
# values (e.g. deleted records).
cdef TripleKey NULL_TRP = [NULL_KEY, NULL_KEY, NULL_KEY]
