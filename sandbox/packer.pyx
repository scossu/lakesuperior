from lakesuperior.cy_include cimport cytpl as tpl
from libc.stdlib cimport malloc, free


cdef:
    tpl.tpl_node *tn
    char *pack
    char *pack2
    size_t size
    size_t size2

cdef struct URIRef:
    unsigned char *data

cdef struct Literal:
    unsigned char type
    unsigned char *data
    unsigned char *datatype
    unsigned char *lang

cdef Literal lit

lit.type = 3
lit.data = b'12345abcde'
lit.datatype = b'xsd:string'
lit.lang = b'en'

tn = tpl.tpl_map("S(csss)", &lit)
tpl.tpl_pack(tn, 0)
tpl.tpl_dump(tn, tpl.TPL_MEM, &pack, &size)
print(f'pack 1: {pack[: size]}')
fmt = tpl.tpl_peek(tpl.TPL_MEM, pack, size)
print(f'pack format: {fmt}')

free(pack)
tpl.tpl_free(tn)

tpl.tpl_jot(tpl.TPL_MEM, &pack2, &size2, <unsigned char *>'S(csss)', &lit)
print(pack2[:size2])

free(pack2)

