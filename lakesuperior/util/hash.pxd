from libc.stdint cimport uint64_t

from lakesuperior.store.ldp_rs.term cimport Buffer


DEF _SEED_LEN = 8 # sizeof(uint64_t)
DEF _HLEN = _SEED_LEN * 2

# Seed for computing the term hash.
#
# This is a 16-byte string that will be split up into two ``uint64``
# numbers to make up the ``spookyhash_128`` seeds.
DEF _TERM_HASH_SEED = b'\xff\xf2Q\xf2j\x0bG\xc1\x8a}\xca\x92\x98^y\x12'

cdef enum:
    SEED_LEN = _SEED_LEN
    HLEN = _HLEN

ctypedef uint64_t Hash64
ctypedef uint64_t DoubleHash64[2]
ctypedef unsigned char Hash128[_HLEN]

cdef:
    uint64_t term_hash_seed1, term_hash_seed2
    unsigned char TERM_HASH_SEED[16]

    int hash128(const Buffer *message, Hash128 *hash) except -1
    int hash64(const Buffer *message, Hash64 *hash) except -1

TERM_HASH_SEED = _TERM_HASH_SEED
