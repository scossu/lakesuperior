from libc.stdint cimport uint32_t, uint64_t

from lakesuperior.store.ldp_rs.term cimport Buffer


# Seed for computing the term hash.
#
# This is a 16-byte string that will be split up into two ``uint64``
# numbers to make up the ``spookyhash_128`` seeds.
#
# TODO This should be made configurable.
DEF _TERM_HASH_SEED = b'\xff\xf2Q\xf2j\x0bG\xc1\x8a}\xca\x92\x98^y\x12'

cdef enum:
    HLEN_32 = sizeof(uint32_t)
    HLEN_64 = sizeof(uint64_t)
    HLEN_128 = sizeof(uint64_t) * 2

ctypedef uint32_t Hash32
ctypedef uint64_t Hash64
ctypedef uint64_t DoubleHash64[2]
ctypedef unsigned char Hash128[HLEN_128]

cdef:
    uint32_t term_hash_seed32
    uint64_t term_hash_seed64_1, term_hash_seed64_2
    unsigned char TERM_HASH_SEED[16]

    int hash32(const Buffer *message, Hash32 *hash) except -1
    int hash64(const Buffer *message, Hash64 *hash) except -1
    int hash128(const Buffer *message, Hash128 *hash) except -1

TERM_HASH_SEED = _TERM_HASH_SEED
