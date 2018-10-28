from libc.stdint cimport uint64_t

# cdefs for serialize and deserialize methods
cdef:
    #unsigned char *pack_data
    unsigned char term_type
    unsigned char *pack_fmt
    unsigned char *term_data
    unsigned char *term_datatype
    unsigned char *term_lang
    #size_t pack_size

    struct IdentifierTerm:
        char type
        unsigned char *data

    struct LiteralTerm:
        char type
        unsigned char *data
        unsigned char *datatype
        unsigned char *lang

    int serialize(term, unsigned char **pack_data, size_t *pack_size) except -1
    deserialize(unsigned char *data, size_t size)


# cdefs for hash methods
DEF _HLEN = 16

ctypedef uint64_t Hash_128[2]
ctypedef unsigned char Hash[_HLEN]

cdef:
    uint64_t term_hash_seed1
    uint64_t term_hash_seed2
    unsigned char *term_hash_seed
    size_t SEED_LEN
    size_t HLEN

    void hash_(
        const unsigned char *message, size_t message_size, Hash *digest)
