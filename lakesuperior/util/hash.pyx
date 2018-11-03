from libc.stdint cimport uint64_t
from libc.string cimport memcpy

term_hash_seed = b'\xff\xf2Q\xf2j\x0bG\xc1\x8a}\xca\x92\x98^y\x12'
"""
Seed for computing the term hash.

This is a 16-byte string that will be split up into two ``uint64``
numbers to make up the ``spookyhash_128`` seeds.
"""
memcpy(&term_hash_seed1, term_hash_seed, SEED_LEN)
memcpy(&term_hash_seed2, term_hash_seed + SEED_LEN, SEED_LEN)

# We only need a couple of functions from spookyhash. No need for a pxd file.
cdef extern from 'spookyhash_api.h':
    void spookyhash_128(
            const void *input, size_t input_size, uint64_t *hash_1,
            uint64_t *hash_2)
    uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed)


cdef inline Hash128 hash128(
        const unsigned char *message, size_t message_size):
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

    spookyhash_128(message, message_size, seed, seed + 1)

    # This casts the 2 contiguous uint64_t's into a char pointer.
    return <Hash128>seed


cdef inline Hash64 hash64(
        const unsigned char *message, size_t message_size):
    """
    Get a 64-bit (unsigned long) hash value of a byte string.

    This function also uses SpookyHash. Note that this returns a UInt64 while
    the 128-bit function returns a char array.
    """
    cdef uint64_t seed = term_hash_seed1

    return spookyhash_64(message, message_size, seed)
