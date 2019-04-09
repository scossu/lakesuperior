__doc__ = """
Basic model typedefs, constants and common methods.
"""

cdef bytes buffer_dump(const Buffer* buf):
    """
    Return a buffer's content as a string.

    :param const Buffer* buf Pointer to a buffer to be read.

    :rtype: str
    """
    cdef unsigned char* buf_stream = (<unsigned char*>buf.addr)
    return buf_stream[:buf.sz]

