from cymem.cymem cimport Pool

from lakesuperior.model.base cimport Buffer

#cdef extern from "regex.h" nogil:
#   ctypedef struct regmatch_t:
#      int rm_so
#      int rm_eo
#   ctypedef struct regex_t:
#      pass
#   int REG_NOSUB, REG_NOMATCH
#   int regcomp(regex_t* preg, const char* regex, int cflags)
#   int regexec(
#       const regex_t *preg, const char* string, size_t nmatch,
#       regmatch_t pmatch[], int eflags
#    )
#   void regfree(regex_t* preg)


ctypedef struct Term:
    char type
    char *data
    char *datatype
    char *lang

cdef:
    #int term_new(
    #    Term* term, char type, char* data, char* datatype=*, char* lang=*
    #) except -1
    #regex_t uri_regex
    # Temporary TPL variable.
    #char* _pk

    int serialize(const Term *term, Buffer *sterm) except -1
    int deserialize(const Buffer *data, Term *term) except -1
    int from_rdflib(term_obj, Term *term) except -1
    int serialize_from_rdflib(term_obj, Buffer *data) except -1
    object deserialize_to_rdflib(const Buffer *data)
    object to_rdflib(const Term *term)
    object to_bytes(const Term *term)

