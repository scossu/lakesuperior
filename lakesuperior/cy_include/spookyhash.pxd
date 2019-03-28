from libc.stdint cimport uint32_t, uint64_t

cdef extern from 'spookyhash_api.h':

    ctypedef struct spookyhash_context:
        pass

    void spookyhash_context_init(
            spookyhash_context *context, uint64_t seed_1, uint64_t seed_2)
    void spookyhash_update(
            spookyhash_context *context, const void *input, size_t input_size)
    void spookyhash_final(
            spookyhash_context *context, uint64_t *hash_1, uint64_t *hash_2)

    uint32_t spookyhash_32(const void *input, size_t input_size, uint32_t seed)
    uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed)
    void spookyhash_128(
            const void *input, size_t input_size, uint64_t *hash_1,
            uint64_t *hash_2)

