from cpython.string cimport va_list
from libc.stdint cimport uint32_t


cdef extern from 'tpl.h':
    ctypedef int tpl_print_fcn(const char *fmt, ...)
    ctypedef void *tpl_malloc_fcn(size_t sz)
    ctypedef void *tpl_realloc_fcn(void *ptr, size_t sz)
    ctypedef void tpl_free_fcn(void *ptr)
    ctypedef void tpl_fatal_fcn(const char *fmt, ...)

    cdef:
        int TPL_FILE
        int TPL_MEM
        int TPL_PREALLOCD
        int TPL_EXCESS_OK
        int TPL_FD
        int TPL_UFREE
        int TPL_DATAPEEK
        int TPL_FXLENS
        int TPL_GETSIZE

        struct tpl_hook_t:
            tpl_print_fcn *oops
            tpl_malloc_fcn *malloc
            tpl_realloc_fcn *realloc
            tpl_free_fcn *free
            tpl_fatal_fcn *fatal
            size_t gather_max

        struct tpl_node:
            int type
            void *addr
            void *data
            int num
            size_t ser_osz
            tpl_node *children
            tpl_node *next
            tpl_node *prev
            tpl_node *parent

        struct tpl_bin:
            void *addr
            uint32_t sz

        struct tpl_gather_t:
            char *img
            int len

    ctypedef int tpl_gather_cb(void *img, size_t sz, void *data)

    # Protoypes.
    tpl_node *tpl_map(char *fmt,...)
    void tpl_free(tpl_node *r)
    int tpl_pack(tpl_node *r, int i)
    int tpl_unpack(tpl_node *r, int i)
    int tpl_dump(tpl_node *r, int mode, ...)
    int tpl_load(tpl_node *r, int mode, ...)
    int tpl_Alen(tpl_node *r, int i)
    char* tpl_peek(int mode, ...)
    int tpl_gather( int mode, ...)
    int tpl_jot(int mode, ...)

    tpl_node *tpl_map_va(char *fmt, va_list ap)

