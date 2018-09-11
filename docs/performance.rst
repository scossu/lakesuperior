Performance Benchmark Report
============================

Environment
-----------

Hardware
~~~~~~~~

‘Rather Snappy’ Laptop
^^^^^^^^^^^^^^^^^^^^^^

-  Dell Latitude 7490 Laptop
-  8x Intel(R) Core(TM) i7-8650U CPU @ 1.90GHz
-  16Gb RAM
-  SSD

‘Ole Workhorse’ server
^^^^^^^^^^^^^^^^^^^^^^

-  8x Intel(R) Xeon(R) CPU X5550 @ 2.67GHz
-  16Gb RAM
-  Magnetic drive, XXX RPM

Software
~~~~~~~~

-  Arch Linux OS
-  glibc 2.26-11
-  python 3.7.0
-  lmdb 0.9.22

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

6'40" running time (only time spent sending requests, not creating the graph)

0.040" per resource (100%—reference point)

3.4M triples total in repo at the end of the process

Retrieval of parent resource (~10000 triples), pipe to /dev/null: 6.22"
(100%)

Peak memory usage: 2.47Gb

Database size: 3.7 Gb

LAKEsuperior Alpha 6, LMDB Back End
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

13’24" running time

0.080" per resource (200%)

Retrieval of parent resource (10K triples), pipe to /dev/null: 2.214"
(35%%)

Peak memory usage: ~650 Mb (3 idle workers, 1 active)

Database size: 523 Mb (16%)

LAKEsuperior experimental branch, Cython + LMDB C-API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

12'37" running time (only time spent sending requests, not creating the graph)

0.075" per resource (168.7%)

Retrieval of parent resource (10K triples), pipe to /dev/null: 2.22"
(35%)

Peak memory usage: ~600 Mb

Database size: 523 Mb (16%)

Performance is only marginally better in spite of the optimization efforts.
Most of the performance penalties are still caused by the RDF parser.

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

**Note:** As it may be obvious, these are only very partial and specific
results. They should not be taken as a thorough performance assessment.
Such an assessment may be impossible and pointless to make given the
very different nature of the storage models, which may behave radically
differently depending on many variables.

Also, this benchmark does not count all the collateral efficienciy advantages
of the 
