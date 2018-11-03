from lakesuperior.cy_include cimport cytpl as tpl

    struct SerializedTriple:
        tpl.tpl_bin s
        tpl.tpl_bin p
        tpl.tpl_bin o


    int serialize(tuple trp, *tpl.tpl_bin data) except -1
    deserialize(tpl.tpl_bin data)
