Data Structure Internals
========================

**(Draft)**

Lakesuperior has its own methods for handling in-memory graphs. These methods
rely on C data structures and are therefore much faster than Python/RDFLib
objects.

The graph data model modules are in :py:module:`lakesuperior.model.graph`.

The Graph Data Model
--------------------

Triples are stored in a C hash set. Each triple is represented by a pointer to
a ``BufferTriple`` structure stored in a temporary memory pool. This pool is
tied to the life cycle of the ``SimpleGraph`` object it belongs to.

A triple structure contains three pointers to ``Buffer`` structures, which
contain a serialized version of a RDF term. These structures are stored in the
``SimpleGraph`` memory pool as well.

Each ``SimpleGraph`` object has a ``_terms`` property and a ``_triples``
property. These are C hash sets holding addresses of unique terms and
triples inserted in the graph. If the same term is entered more than once,
in any position in any triple, the first one entered is used and is pointed to
by the triple. This makes the graph data structure very compact.

In summary, the pointers can be represented this way::

   <serialized term data in mem pool (x3)>
         ^      ^      ^
         |      |      |
   <Term structures in mem pool (x3)>
         ^      ^      ^
         |      |      |
   <Term struct addresses in _terms set (x3)>
         ^      ^      ^
         |      |      |
   <Triple structure in mem pool>
         ^
         |
   <address of triple in _triples set>

Let's say we insert the following triples in a ``SimpleGraph``::

   <urn:s:0> <urn:p:0> <urn:o:0>
   <urn:s:0> <urn:p:1> <urn:o:1>
   <urn:s:0> <urn:p:1> <urn:o:2>
   <urn:s:0> <urn:p:0> <urn:o:0>

The memory pool contains the following byte arrays  of raw data, displayed in
the following list with their relative addresses (simplified to 8-bit
addresses and fixed-length byte strings for readability)::

   0x00     <urn:s:0>
   0x09     <urn:p:0>
   0x12     <urn:o:0>

   0x1b     <urn:s:0>
   0x24     <urn:p:1>
   0x2d     <urn:o:1>

   0x36     <urn:s:0>
   0x3f     <urn:p:1>
   0x48     <urn:o:2>

   0x51     <urn:s:0>
   0x5a     <urn:p:0>
   0x63     <urn:o:0>

However, the ``_terms`` set contains only ``Buffer`` structures pointing to
unique addresses::

   0x00
   0x09
   0x12
   0x24
   0x2d
   0x48

The other terms are just unutilized. They will be deallocated en masse when
the ``SimpleGraph`` object is garbage collected.

The ``_triples`` set would then contain 3 unique entries pointing to the unique
term addresses::

   0x00  0x09  0x12
   0x00  0x24  0x2d
   0x00  0x24  0x48

(the actual addresses would actually belong to the structures pointing to the
raw data, but this is just an illustrative example).

The advantage of this approach is that the memory pool is contiguous and
append-only (until it gets purged), so it's cheap to just add to it, while the
sets that must maintain uniqueness and are the ones that most operations
(lookup, adding, removing, slicing, copying, etc.) are done on, contain much
less data and are therefore faster.
