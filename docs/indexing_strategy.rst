RDF Store & Index Design
========================

This is a log of subsequent strategies employed to store triples in
LMDB.

Strategy #4a is the one currently used. The rest is kept for historic
reasons and academic curiosity (and also because this took too much work to
just wipe out of memory).

Storage approach
----------------

-  Pickle quad and create MD5 or SHA1 hash.
-  Store triples in one database paired with key; store indices
   separately.

Different strategies involve layout and number of databases.

Strategy #1
-----------

-  kq: key: serialized triple (1:1)
-  sk: Serialized subject: key (1:m)
-  pk: Serialized predicate: key (1:m)
-  ok: Serialized object: key (1:m)
-  (optional) lok: Serialized literal object: key (1:m)
-  (optional) tok: Serialized RDF type: key (1:m)
-  ck: Serialized context: key (1:m)

Retrieval approach
~~~~~~~~~~~~~~~~~~

To find all matches for a quad:

-  If all terms in the quad are bound, generate the key from the pickled
   quad and look up the triple in ``kt``
-  If all terms are unbound, return an iterator of all values in ``kt``.
-  If some values are bound and some unbound (most common query):

   -  Get a base list of keys associated wirh the first bound term
   -  For each subsequent bound term, check if each key associated with
      the term matches a key in the base list
   -  Continue through all the bound terms. If a match is not found at
      any point, continue to the next term
   -  If a match is found in all the bound term databases, look up the
      pickled quad matching the key in ``kq`` and yield it

More optimization can be introduced later, e.g. separating literal and
RDF type objects in separate databases. Literals can have very long
values and a database with a longer key setting may be useful. RDF terms
can be indexed separately because they are the most common bound term.

Example lookup
~~~~~~~~~~~~~~

Keys and Triples (should actually be quads but this is a simplified
version):

- A:
  - s1
  - p1
  - o1
- B:
  - s1
  - p2
  - o2
- C:
  - s2
  - p3
  - o1
- D:
  - s2
  - p3
  - o3

Indices:

-  SK:

   -  s1: A, B
   -  s2: C, D

-  PK:

   -  p1: A
   -  p2: B
   -  p3: C, D

-  OK:
-  o1: A, C
-  o2: B
-  o3: D

Queries:

-  s1 ?p ?o → {A, B}
-  s1 p2 ?o → {A, B} & {B} = {B}
-  ?s ?p o3 → {D}
-  s1 p2 o5 → {} (Exit at OK: no term matches ‘o5’)
-  s2 p3 o2 → {C, D} & {C, D} & {B} = {}

Strategy #2
-----------

Separate data and indices in two environments.

Main data store
~~~~~~~~~~~~~~~

Key to quad; main keyspace; all unique.

Indices
~~~~~~~

None of these databases is of critical preservation concern. They can be
rebuilt from the main data store.

All dupsort and dupfixed.

@TODO The first three may not be needed if computing term hash is fast
enough.

-  t2k (term to term key)
-  lt2k (literal to term key: longer keys)
-  k2t (term key to term)

-  s2k (subject key to quad key)
-  p2k (pred key to quad key)
-  o2k (object key to quad key)
-  c2k (context key to quad key)

-  sc2qk (subject + context keys to quad key)
-  po2qk (predicate + object keys to quad key)

-  sp2qk (subject + predicate keys to quad key)
-  oc2qk (object + context keys to quad key)

-  so2qk (subject + object keys to quad key)
-  pc2qk (predicate + context keys to quad key)

Strategy #3
-----------

Contexts are much fewer (even in graph per aspect, 5-10 triples per
graph)

.. _main-data-store-1:

Main data store
~~~~~~~~~~~~~~~

Preservation-worthy data

-  tk:t (triple key: triple; dupsort, dupfixed)
-  tk:c (context key: triple; unique)

.. _indices-1:

Indices
~~~~~~~

Rebuildable from main data store

-  s2k (subject key: triple key)
-  p2k (pred key: triple key)
-  o2k (object key: triple key)
-  sp2k
-  so2k
-  po2k
-  spo2k

Lookup
~~~~~~

1. Look up triples by s, p, o, sp, so, po and get keys
2. If a context is specified, for each key try to seek to (context, key)
   in ct to verify it exists
3. Intersect sets
4. Match triple keys with data using kt

Shortcuts
^^^^^^^^^

-  Get all contexts: return list of keys from ct
-  Get all triples for a context: get all values for a contex from ct
   and match triple data with kt
-  Get one triple match for all contexts: look up in triple indices and
   match triple data with kt

Strategy #4
-----------

Terms are entered individually in main data store. Also, shorter keys
are used rather than hashes. These two aspects save a great deal of
space and I/O, but require an additional index to put the terms together
in a triple.

.. _main-data-store-2:

Main Data Store
~~~~~~~~~~~~~~~

-  t:st (term key: serialized term; 1:1)
-  spo:c (joined S, P, O keys: context key; 1:m)
-  c: (context keys only, values are the empty bytestring)

Storage total: variable

.. _indices-2:

Indices
~~~~~~~

-  th:t (term hash: term key; 1:1)
-  c:spo (context key: joined triple keys; 1:m)
-  s:po (S key: P + O key; 1:m)
-  p:so (P key: S + O keys; 1:m)
-  o:sp (object key: triple key; 1:m)
-  sp:o (S + P keys: O key; 1:m)
-  so:p (S + O keys: P key; 1:m)
-  po:s (P + O keys: S key; 1:m)

Storage total: 143 bytes per triple

Disadvantages
~~~~~~~~~~~~~

-  Lots of indices
-  Terms can get orphaned:

   -  No easy way to know if a term is used anywhere in a quad
   -  Needs some routine cleanup
   -  On the other hand, terms are relatively light-weight and can be
      reused
   -  Almost surely not reusable are UUIDs, message digests, timestamps
      etc.

Strategy #5
-----------

Reduce number of indices and rely on parsing and splitting keys to find
triples with two bound parameters.

This is especially important for keeping indexing synchronous to achieve
fully ACID writes.

.. _main-data-store-3:

Main data store
~~~~~~~~~~~~~~~

Same as Strategy #4:

-  t:st (term key: serialized term; 1:1)
-  spo:c (joined S, P, O keys: context key; dupsort, dupfixed)
-  c: (context keys only, values are the empty bytestring; 1:1)

Storage total: variable (same as #4)

.. _indices-3:

Indices
~~~~~~~

-  th:t (term hash: term key; 1:1)
-  s:po (S key: joined P, O keys; dupsort, dupfixed)
-  p:so (P key: joined S, O keys; dupsort, dupfixed)
-  o:sp (O key: joined S, P keys; dupsort, dupfixed)
-  c:spo (context → triple association; dupsort, dupfixed)

Storage total: 95 bytes per triple

Lookup strategy
~~~~~~~~~~~~~~~

-  ? ? ? c: [c:spo] all SPO for C → split key → [t:st] term from term
   key
-  s p o c: [c:spo] exact SPO & C match → split key → [t:st] term from
   term key
-  s ? ?: [s:po] All PO for S → split key → [t:st] term from term key
-  s p ?: [s:po] All PO for S → filter result by P in split key → [t:st]
   term from term key

Advantages
~~~~~~~~~~

-  Less indices: smaller index size and less I/O

.. _disadvantages-1:

Disadvantages
~~~~~~~~~~~~~

-  Slower retrieval for queries with 2 bound terms

Further optimization
~~~~~~~~~~~~~~~~~~~~

In order to minimize traversing and splittig results, the first
retrieval should be made on the term with less average keys. Search
order can be balanced by establishing a lookup order for indices.

This can be achieved by calling stats on the index databases and looking
up the database with *most* keys. Since there is an equal number of
entries in each of the (s:po, p:so, o:sp) indices, the one with most
keys will have the least average number of values per key. If that
lookup is done first, the initial data set to traverse and filter will
be smaller.

Strategy #5a
------------

This is a slightly different implementation of #5 that somewhat
simplifies and perhaps speeds up things a bit.
The indexing and lookup strtegy is the same; but instead of using a
separator byte for splitting compound keys, the logic relies on the fact
that keys have a fixed length and are sliced instead. This *should*
result in faster key manipulation, also because in most cases
``memoryview`` buffers can be used directly instead of being copied from
memory.

Index storage is 90 bytes per triple.

Strategy #4a
------------

This is a variation of Strategy 4 using fixed-size keys. It is the currently
employed solution starting with alpha18.

After using #5a up to alpha17, it was apparent that 2-bound queries were quite
penalized in queries which return few results. All the keys for a 1-bound
lookup had to be retrieved and iterated over to verify that they contained the
second ("filter") term. This approach, instead, only looks up the relevant
keys and composes the results. It is slower on writes and nearly doubles the
size of the indices, but it makes reads faster and more memory-efficient.

