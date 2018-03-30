Performance Benchmark Report
============================

Environment
-----------

Hardware
~~~~~~~~

‘Rather Snappy’ Laptop
^^^^^^^^^^^^^^^^^^^^^^

-  Dell Precison M3800 Laptop
-  4x Intel(R) Core(TM) i7-4712HQ CPU @ 2.30GHz
-  12Gb RAM
-  SSD

‘Ole Workhorse’ server
^^^^^^^^^^^^^^^^^^^^^^

8x Intel(R) Xeon(R) CPU X5550 @ 2.67GHz 16Gb RAM Magnetic drive, XXX RPM

Software
~~~~~~~~

-  Arch Linux OS
-  glibc 2.26-11
-  python 3.5.4
-  lmdb 0.9.21-1

Benchmark script
~~~~~~~~~~~~~~~~

`Generator script <../../util/benchmark.py>`__

The script was run with default values: 10,000 children under the same
parent, PUT requests.

Data Set
~~~~~~~~

Synthetic graph created by the benchmark script. The graph is unique for
each request and consists of 200 triples which are partly random data,
with a consistent size and variation:

-  50 triples have an object that is a URI of an external resource (50
   unique predicates; 5 unique objects).
-  50 triples have an object that is a URI of a repository-managed
   resource (50 unique predicates; 5 unique objects).
-  100 triples have an object that is a 64-character random Unicode
   string (50 unique predicates; 100 unique objects).

Results
-------

.. _rather-snappy-laptop-1:

‘Rather Snappy’ Laptop
~~~~~~~~~~~~~~~~~~~~~~

FCREPO/Modeshape 4.7.5
^^^^^^^^^^^^^^^^^^^^^^

15’45" running time

0.094" per resource (100%—reference point)

3.4M triples total in repo at the end of the process

Retrieval of parent resource (~10000 triples), pipe to /dev/null: 3.64"
(100%)

Peak memory usage: 2.47Gb

Database size: 3.3 Gb

LAKEsuperior Alpha 6, LMDB Back End
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

25’ running time

0.152" per resource (161%)

*Some gaps every ~40-50 requests, probably disk flush*

Retrieval of parent resource (10K triples), pipe to /dev/null: 2.13"
(58%)

Peak memory usage: ~650 Mb (3 idle workers, 1 active)

Database size: 523 Mb (16%)

.. _ole-workhorse-server-1:

‘Ole Workhorse’ server
~~~~~~~~~~~~~~~~~~~~~~

FCREPO
^^^^^^

0:47:38 running time

0.285" per resource (100%)

Retrieval of parent resource: 9.6" (100%)

LAKEsuperior
^^^^^^^^^^^^

1:14:19 running time

0.446" per resource (156%)

Retrieval of parent resource: 5.58" (58%)

Conclusions
-----------

LAKEsuperior appears to be markedly slower on writes and markedly faster
on reads. Both these factors are very likely related to the underlying
LMDB store which is optimized for read performance.

Comparison of results between the laptop and the server demonstrates
that both read and write performance gaps are identical in the two
environments. Disk speed severely affects the numbers.

**Note:** As you can guess, these are only very partial and specific
results. They should not be taken as a thorough performance assessment.
Such an assessment may be impossible and pointless to make given the
very different nature of the storage models, which may behave radically
differently depending on many variables.
