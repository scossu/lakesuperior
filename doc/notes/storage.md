# Storage implementation

LAKEsuperior stores non-RDF ("binary") data in the filesystem and RDF data in
an embedded key-value store, [LMDB](https://symas.com/lmdb/).

## RDF Storage design

LMDB is a very fast, very lightweight C library. It is inspired by BerkeleyDB
but introduces significant improvements in terms of efficiency and stability.

The LAKEsuperior RDF store consists of two files: the main data store and the
indices (plus two lock files that are generated at runtime). A good amount of
effort has been put to develop an indexing strategy that is balanced between
write performance, read performance, and data size, with no compromise made on
consistency.

The main data
store is the one containing the preservation-worthy data. While the indices are
necessary for LAKEsuperior to function, they can be entirely rebuilt from the
main data store in case of file corruption (recovery tools are on the TODO
list).

## Scalability

Since LAKEsuperior is focused on design simplicity, efficiency and reliability,
its RDF store is embedded and not horizontally scalable. However, LAKEsuperior
is quite frugal with disk space. About 55 million triples can be
stored in 8Gb of space (mileage can vary depending on how heterogeneous the
triples are). This makes it easier to use expensive SSD drives for
the RDF store, in order to improve performance. A single LMDB environment can
reportedly scale up to 128 terabytes.

## Maintenance

LMDB has a very simple configuration, and all options are hardcoded
in LAKESuperior in order to exploit its features. A database automatically
recovers from a crash.

The LAKEsuperior RDF store abstraction maintains a registry of unique terms.
These terms are not deleted if a triple is deleted, even if no triple is using
them, because it would be too expesive to look up for orphaned terms during a
delete request. While these terms are relatively lightweight, it would be good
to run a periodical clean-up job. Tools will be developed in the near future to
facilitate this maintenance task.

## Consistency

LAKEsuperior wraps each LDP operation in a transaction. The indices are updated
synchronously within the same transaction in order to guarantee
consistency. If a system loses power or crashes, only the last transaction is
lost, and the last successful write will include primary and index data.

## Concurrency

LMDB employs
[MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
to achieve fully ACID transactions. This implies that during
a write, the whole database is locked. Multiple writes can be initiated
concurrently, but the performance gain of doing so may be little because
only one write operation can be performed at a time. Reasonable efforts have
been put to make write transactions as short as possible (and more can be
done). Also, this excludes a priori the option to implement long-running atomic
operations, unless one is willing to block writes on the application for an
indefinite length of time. On the other hand, write operations never block and
are never blocked, so an application with a high read-to-write ratio may still
benefit from multi-threaded requests.

## Performance

The [Performance Benchmark Report](performance.txt) contains benchmark results.

Write performance is lower than Modeshape/Fedora4; this may be mostly due to
the fact that indices are written synchronously in a blocking transaction;
also, the LMDB B+Tree structure is optimized for read performance rather than
write performance. Some optimizations on the application layer could be made.

Reads are faster than Modeshape/Fedora.

All tests so far have been performed in a single thread.
