# cython: language_level = 3

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

    IdentifierTerm id_t
    LiteralTerm lit_t

    int serialize(term, unsigned char **pack_data, size_t *pack_size) except -1
    deserialize(unsigned char *data, size_t size)

