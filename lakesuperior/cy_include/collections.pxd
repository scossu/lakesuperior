from libc.stdint cimport uint32_t

ctypedef void* (*mem_alloc_ft)(size_t size)
ctypedef void* (*mem_calloc_ft)(size_t blocks, size_t size)
ctypedef void (*mem_free_ft)(void* block)
ctypedef size_t (*hash_ft)(const void* key, int l, uint32_t seed)
ctypedef int (*key_compare_ft)(const void* key1, const void* key2)


cdef extern from "common.h":

    enum cc_stat:
        CC_OK
        CC_ERR_ALLOC
        CC_ERR_INVALID_CAPACITY
        CC_ERR_INVALID_RANGE
        CC_ERR_MAX_CAPACITY
        CC_ERR_KEY_NOT_FOUND
        CC_ERR_VALUE_NOT_FOUND
        CC_ERR_OUT_OF_RANGE
        CC_ITER_END

    key_compare_ft CC_CMP_STRING
    key_compare_ft CC_CMP_POINTER
#
#    int cc_common_cmp_str(const void* key1, const void* key2)
#
#    int cc_common_cmp_ptr(const void* key1, const void* key2)

cdef extern from "array.h":

    ctypedef struct Array:
        pass

    ctypedef struct ArrayConf:
        size_t          capacity
        float           exp_factor
        mem_alloc_ft  mem_alloc
        mem_calloc_ft mem_calloc
        mem_free_ft   mem_free

    ctypedef struct ArrayIter:
        Array* ar
        size_t index
        bint last_removed

#    ctypedef struct ArrayZipIter:
#        Array* ar1
#        Array* ar2
#        size_t index
#        bint last_removed
#
    cc_stat array_new(Array** out)

    cc_stat array_new_conf(ArrayConf* conf, Array** out)

    void array_conf_init(ArrayConf* conf)

    void array_destroy(Array* ar)

#    ctypedef void (*_array_destroy_cb_cb_ft)(void*)
#
#    void array_destroy_cb(Array* ar, _array_destroy_cb_cb_ft cb)
#
    cc_stat array_add(Array* ar, void* element)
#
#    #cc_stat array_add_at(Array* ar, void* element, size_t index)
#
#    cc_stat array_replace_at(Array* ar, void* element, size_t index, void** out)
#
#    cc_stat array_swap_at(Array* ar, size_t index1, size_t index2)
#
#    cc_stat array_remove(Array* ar, void* element, void** out)
#
#    cc_stat array_remove_at(Array* ar, size_t index, void** out)
#
#    cc_stat array_remove_last(Array* ar, void** out)
#
#    void array_remove_all(Array* ar)
#
#    void array_remove_all_free(Array* ar)
#
#    cc_stat array_get_at(Array* ar, size_t index, void** out)
#
#    cc_stat array_get_last(Array* ar, void** out)
#
#    cc_stat array_subarray(Array* ar, size_t from_, size_t to, Array** out)
#
#    cc_stat array_copy_shallow(Array* ar, Array** out)
#
#    ctypedef void* (*_array_copy_deep_cp_ft)(void*)
#
#    cc_stat array_copy_deep(Array* ar, _array_copy_deep_cp_ft cp, Array** out)
#
#    void array_reverse(Array* ar)
#
#    cc_stat array_trim_capacity(Array* ar)
#
#    size_t array_contains(Array* ar, void* element)
#
#    ctypedef int (*_array_contains_value_cmp_ft)(void*, void*)
#
#    size_t array_contains_value(Array* ar, void* element, _array_contains_value_cmp_ft cmp)
#
#    size_t array_size(Array* ar)
#
#    size_t array_capacity(Array* ar)
#
#    cc_stat array_index_of(Array* ar, void* element, size_t* index)
#
#    ctypedef int (*_array_sort_cmp_ft)(void*, void*)
#
#    void array_sort(Array* ar, _array_sort_cmp_ft cmp)
#
#    ctypedef void (*_array_map_fn_ft)(void*)
#
#    void array_map(Array* ar, _array_map_fn_ft fn)
#
#    ctypedef void (*_array_reduce_fn_ft)(void*, void*, void*)
#
#    void array_reduce(Array* ar, _array_reduce_fn_ft fn, void* result)
#
#    ctypedef bint (*_array_filter_mut_predicate_ft)(void*)
#
#    cc_stat array_filter_mut(Array* ar, _array_filter_mut_predicate_ft predicate)
#
#    ctypedef bint (*_array_filter_predicate_ft)(void*)
#
#    cc_stat array_filter(Array* ar, _array_filter_predicate_ft predicate, Array** out)
#
    void array_iter_init(ArrayIter* iter, Array* ar)

    cc_stat array_iter_next(ArrayIter* iter, void** out)
#
#    cc_stat array_iter_remove(ArrayIter* iter, void** out)
#
#    cc_stat array_iter_add(ArrayIter* iter, void* element)
#
#    cc_stat array_iter_replace(ArrayIter* iter, void* element, void** out)
#
#    size_t array_iter_index(ArrayIter* iter)
#
#    void array_zip_iter_init(ArrayZipIter* iter, Array* a1, Array* a2)
#
#    cc_stat array_zip_iter_next(ArrayZipIter* iter, void** out1, void** out2)
#
#    cc_stat array_zip_iter_add(ArrayZipIter* iter, void* e1, void* e2)
#
#    cc_stat array_zip_iter_remove(ArrayZipIter* iter, void** out1, void** out2)
#
#    cc_stat array_zip_iter_replace(ArrayZipIter* iter, void* e1, void* e2, void** out1, void** out2)
#
#    size_t array_zip_iter_index(ArrayZipIter* iter)
#
#    void** array_get_buffer(Array* ar)


cdef extern from "hashtable.h":

    ctypedef struct TableEntry:
        void*       key
        void*       value
        size_t      hash
        TableEntry* next

    ctypedef struct HashTable:
        pass

    ctypedef struct HashTableConf:
        float               load_factor
        size_t              initial_capacity
        int                 key_length
        uint32_t            hash_seed

        hash_ft           hash
        key_compare_ft    key_compare
        mem_alloc_ft  mem_alloc
        mem_calloc_ft mem_calloc
        mem_free_ft   mem_free

    ctypedef struct HashTableIter:
        HashTable* table
        size_t bucket_index
        TableEntry* prev_entry
        TableEntry* next_entry

    hash_ft GENERAL_HASH
    hash_ft STRING_HASH
    hash_ft POINTER_HASH

#    size_t get_table_index(HashTable *table, void *key)
#
#    void hashtable_conf_init(HashTableConf* conf)
#
#    cc_stat hashtable_new(HashTable** out)
#
#    cc_stat hashtable_new_conf(HashTableConf* conf, HashTable** out)
#
#    void hashtable_destroy(HashTable* table)
#
#    cc_stat hashtable_add(HashTable* table, void* key, void* val)
#
#    cc_stat hashtable_get(HashTable* table, void* key, void** out)
#
#    cc_stat hashtable_remove(HashTable* table, void* key, void** out)
#
#    void hashtable_remove_all(HashTable* table)
#
#    bint hashtable_contains_key(HashTable* table, void* key)
#
#    size_t hashtable_size(HashTable* table)
#
#    size_t hashtable_capacity(HashTable* table)
#
#    cc_stat hashtable_get_keys(HashTable* table, Array** out)
#
#    cc_stat hashtable_get_values(HashTable* table, Array** out)
#
    size_t hashtable_hash_string(void* key, int len, uint32_t seed)

    size_t hashtable_hash(void* key, int len, uint32_t seed)

    size_t hashtable_hash_ptr(void* key, int len, uint32_t seed)
#
#    ctypedef void (*_hashtable_foreach_key_op_ft)(void*)
#
#    void hashtable_foreach_key(HashTable* table, _hashtable_foreach_key_op_ft op)
#
#    ctypedef void (*_hashtable_foreach_value_op_ft)(void*)
#
#    void hashtable_foreach_value(HashTable* table, _hashtable_foreach_value_op_ft op)
#
#    void hashtable_iter_init(HashTableIter* iter, HashTable* table)
#
#    cc_stat hashtable_iter_next(HashTableIter* iter, TableEntry** out)
#
#    cc_stat hashtable_iter_remove(HashTableIter* iter, void** out)


cdef extern from "hashset.h":

    ctypedef struct HashSet:
        pass

    ctypedef HashTableConf HashSetConf

    ctypedef struct HashSetIter:
        HashTableIter iter

    void hashset_conf_init(HashSetConf* conf)

    cc_stat hashset_new(HashSet** hs)

    cc_stat hashset_new_conf(HashSetConf* conf, HashSet** hs)

    void hashset_destroy(HashSet* set)

    cc_stat hashset_add(HashSet* set, void* element)

    cc_stat hashset_add_or_get(HashSet* set, void** element)

    cc_stat hashset_remove(HashSet* set, void* element, void** out)

    void hashset_remove_all(HashSet* set)

    bint hashset_contains(HashSet* set, void* element)

    cc_stat hashset_get(HashSet *set, void **element)

    size_t hashset_size(HashSet* set)

    size_t hashset_capacity(HashSet* set)

    ctypedef void (*_hashset_foreach_op_ft)(void*)

    void hashset_foreach(HashSet* set, _hashset_foreach_op_ft op)

    void hashset_iter_init(HashSetIter* iter, HashSet* set)

    cc_stat hashset_iter_next(HashSetIter* iter, void** out)

    cc_stat hashset_iter_remove(HashSetIter* iter, void** out)
