cdef extern from 'set.h':
    #ctypedef _Set Set
    #ctypedef _SetEntry SetEntry
    ctypedef void *SetValue

    ctypedef unsigned int (*SetHashFunc)(SetValue value)
    ctypedef bint (*SetEqualFunc)(SetValue value1, SetValue value2)
    ctypedef void (*SetFreeFunc)(SetValue value)

    ctypedef struct SetEntry:
        SetValue data
        SetEntry *next

    ctypedef struct _Set:
        SetEntry **table
        unsigned int entries
        unsigned int table_size
        unsigned int prime_index
        SetHashFunc hash_func
        SetEqualFunc equal_func
        SetFreeFunc free_func

    ctypedef _Set Set

    ctypedef struct SetIterator:
        pass

    Set *set_new(SetHashFunc hash_func, SetEqualFunc equal_func)
    void set_free(Set *set)
    # TODO This should return an int, ideally. See
    # https://github.com/fragglet/c-algorithms/issues/20
    bint set_insert(Set *set, SetValue data)
    bint set_insert_or_assign(Set *set, SetValue *data)
    bint set_query(Set *set, SetValue data)
    bint set_enlarge(Set *set)
    unsigned int set_num_entries(Set *set)
    SetValue *set_to_array(Set *set)
    Set *set_union(Set *set1, Set *set2)
    Set *set_intersection(Set *set1, Set *set2)
    void set_iterate(Set *set, SetIterator *iter)
    bint set_iter_has_more(SetIterator *iterator)
    SetValue set_iter_next(SetIterator *iterator)


cdef extern from 'hash-table.h':
    ctypedef void *HashTableKey
    ctypedef void *HashTableValue

    ctypedef struct HashTablePair:
        HashTableKey key
        HashTableKey value

    ctypedef struct HashTableEntry:
        HashTablePair pair
        HashTableEntry *next

    ctypedef struct HashTable:
        HashTableEntry **table
        unsigned int table_size
        unsigned int entries
        unsigned int prime_index

    ctypedef struct HashTableIterator:
        pass

    ctypedef unsigned int (*HashTableHashFunc)(HashTableKey value)
    ctypedef bint (*HashTableEqualFunc)(
            HashTableKey value1, HashTableKey value2)
    ctypedef void (*HashTableKeyFreeFunc)(HashTableKey value)
    ctypedef void (*HashTableValueFreeFunc)(HashTableValue value)


    HashTable *hash_table_new(
            HashTableHashFunc hash_func, HashTableEqualFunc equal_func)
    void hash_table_free(HashTable *hash_table)
    void hash_table_register_free_functions(
            HashTable *hash_table, HashTableKeyFreeFunc key_free_func,
            HashTableValueFreeFunc value_free_func)
    int hash_table_insert(
            HashTable *hash_table, HashTableKey key, HashTableValue value)
    HashTableValue hash_table_lookup(
            HashTable *hash_table, HashTableKey key)
    bint hash_table_remove(HashTable *hash_table, HashTableKey key)
    unsigned int hash_table_num_entries(HashTable *hash_table)
    void hash_table_iterate(HashTable *hash_table, HashTableIterator *iter)
    bint hash_table_iter_has_more(HashTableIterator *iterator)
    HashTablePair hash_table_iter_next(HashTableIterator *iterator)

