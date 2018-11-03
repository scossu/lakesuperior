cdef extern from 'set.h':
    #ctypedef _Set Set
    #ctypedef _SetEntry SetEntry
    ctypedef void *SetValue

    ctypedef struct SetEntry:
        SetValue data
        SetEntry *next

    ctypedef struct Set:
        SetEntry **table
        unsigned int entries
        unsigned int table_size
        unsigned int prime_index
        #SetHashFunc hash_func
        #SetEqualFunc equal_func
        #SetFreeFunc free_func

    ctypedef struct SetIterator:
        pass

    ctypedef unsigned int (*SetHashFunc)(SetValue value)
    ctypedef bint (*SetEqualFunc)(SetValue value1, SetValue value2)
    ctypedef void (*SetFreeFunc)(SetValue value)

    Set *set_new(SetHashFunc hash_func, SetEqualFunc equal_func)
    void set_free(Set *set)
    # TODO This should return an int, ideally. See
    # https://github.com/fragglet/c-algorithms/issues/20
    bint set_insert(Set *set, SetValue data)
    bint set_query(Set *set, SetValue data)
    unsigned int set_num_entries(Set *set)
    SetValue *set_to_array(Set *set)
    Set *set_union(Set *set1, Set *set2)
    Set *set_intersection(Set *set1, Set *set2)
    void set_iterate(Set *set, SetIterator *iter)
    bint set_iter_has_more(SetIterator *iterator)
    SetValue set_iter_next(SetIterator *iterator)


