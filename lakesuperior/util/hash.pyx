from libc.stdint cimport uint64_t
from libc.string cimport memcpy

from lakesuperior.store.ldp_rs.term cimport Buffer


memcpy(&term_hash_seed1, TERM_HASH_SEED, SEED_LEN)
memcpy(&term_hash_seed2, TERM_HASH_SEED + SEED_LEN, SEED_LEN)

# We only need a couple of functions from spookyhash. No need for a pxd file.
cdef extern from 'spookyhash_api.h':
    void spookyhash_128(
            const void *input, size_t input_size, uint64_t *hash_1,
            uint64_t *hash_2)
    uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed)


cdef inline int hash128(const Buffer *message, Hash128 *hash) except -1:
    """
    Get the hash value of a byte string with a defined size.

    The hashing algorithm is `SpookyHash
    <http://burtleburtle.net/bob/hash/spooky.html>`_ which produces 128-bit
    (16-byte) digests.

    The initial seeds are determined in the application configuration.

    :rtype: Hash128
    """
    cdef:
        DoubleHash64 seed = [term_hash_seed1, term_hash_seed2]
        Hash128 digest

    spookyhash_128(message[0].addr, message[0].sz, seed, seed + 1)

    # This casts the 2 contiguous uint64_t's into a char[16] pointer.
    hash[0] = <Hash128>seed


cdef inline int hash64(const Buffer *message, Hash64 *hash) except -1:
    """
    Get a 64-bit (unsigned long) hash value of a byte string.

    This function also uses SpookyHash. Note that this returns a UInt64 while
    the 128-bit function returns a char array.
    """
    cdef uint64_t seed = term_hash_seed1

    hash[0] = spookyhash_64(message[0].addr, message[0].sz, seed)
