from libc.stdint cimport uint64_t


DEF _SEED_LEN = 8 # sizeof(uint64_t)
DEF _HLEN = _SEED_LEN * 2

cdef enum:
    SEED_LEN = _SEED_LEN
    HLEN = _HLEN

ctypedef uint64_t Hash64
ctypedef uint64_t DoubleHash64[2]
ctypedef unsigned char Hash128[_HLEN]

cdef:
    uint64_t term_hash_seed1
    uint64_t term_hash_seed2SetValue
    unsigned char *term_hash_seed

    Hash128 hash128(
        const unsigned char *message, size_t message_size)
    Hash64 hash64(
        const unsigned char *message, size_t message_size)

