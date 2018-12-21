from libc.stdint cimport uint64_t
from libc.string cimport memcpy

from lakesuperior.store.ldp_rs.term cimport Buffer


memcpy(&term_hash_seed32, TERM_HASH_SEED, HLEN_32)
memcpy(&term_hash_seed64_1, TERM_HASH_SEED, HLEN_64)
memcpy(&term_hash_seed64_2, TERM_HASH_SEED + HLEN_64, HLEN_64)

# We only need a few basic functions from spookyhash. No need for a pxd file.
cdef extern from 'spookyhash_api.h':
    uint32_t spookyhash_32(const void *input, size_t input_size, uint32_t seed)
    uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed)
    void spookyhash_128(
            const void *input, size_t input_size, uint64_t *hash_1,
            uint64_t *hash_2)


cdef inline int hash32(const Buffer *message, Hash32 *hash) except -1:
    """
    Get a 32-bit (unsigned int) hash value of a byte string.
    """
    cdef uint32_t seed = term_hash_seed64_1

    hash[0] = spookyhash_32(message[0].addr, message[0].sz, seed)


cdef inline int hash64(const Buffer *message, Hash64 *hash) except -1:
    """
    Get a 64-bit (unsigned long) hash value of a byte string.
    """
    cdef uint64_t seed = term_hash_seed32

    hash[0] = spookyhash_64(message[0].addr, message[0].sz, seed)


cdef inline int hash128(const Buffer *message, Hash128 *hash) except -1:
    """
    Get the hash value of a byte string with a defined size.

    The hashing algorithm is `SpookyHash
    <http://burtleburtle.net/bob/hash/spooky.html>`_ which produces 128-bit
    (16-byte) digests.

    Note that this returns a char array while the smaller functions return
    numeric types (uint, ulong).

    The initial seeds are determined in the application configuration.

    :rtype: Hash128
    """
    cdef:
        DoubleHash64 seed = [term_hash_seed64_1, term_hash_seed64_2]
        Hash128 digest

    spookyhash_128(message[0].addr, message[0].sz, seed, seed + 1)

    # This casts the 2 contiguous uint64_t's into a char[16] pointer.
    hash[0] = <Hash128>seed
