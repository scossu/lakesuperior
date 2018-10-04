Performance Benchmark Report
============================

The purpose of this document is to provide very broad performance measurements
and comparison between Lakesuperior and Fedora/Modeshape implementations.

Lakesuperior v1.0a17 and v1.0a18 were taken into consideration. This is because
of the extensive reworking of the whole architecture and complete rewrite
of the storage layer, that led to significant performance gains.

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
-  Arch Linux OS
-  glibc 2.26-11
-  python 3.7.0
-  lmdb 0.9.22

The laptop was left alone during the process, but some major applications
(browser, email client, etc.) were left open.

‘Ole Workhorse’ server
^^^^^^^^^^^^^^^^^^^^^^

-  8x Intel(R) Xeon(R) CPU X5550 @ 2.67GHz
-  16Gb RAM
-  Magnetic drive, XXX RPM

Benchmark script
~~~~~~~~~~~~~~~~

`Generator script <../../util/benchmark.py>`__

The script was run with default values: resprectively 10,000 and 100,000
children under the same parent. PUT and POST requests were tested separately.

The script calculates only the timings used for the PUT or POST requests, not
counting the time used to generate the graphs.

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

LDP Data Retrieval
~~~~~~~~~~~~~~~~~~

REST API request::

   time curl http://localhost:8000/ldp/pomegranate > /dev/null

SPARQL Query
~~~~~~~~~~~~

*Note:* The query may take a long time and therefore is made on the
single-threaded server (``lsup-server``) that does not impose a timeout (of
course, gunicorn could also be used by changing the configuration to allow a
long timeout).

Sample query::

   PREFIX ldp: <http://www.w3.org/ns/ldp#>
   SELECT (COUNT(?s) AS ?c) WHERE {
     ?s a ldp:Resource .
     ?s a ldp:Container .
   }

Raw request::

   time curl -iXPOST -H'Accept:application/sparql-results+json' \
   -H'Content-Type:application/x-www-form-urlencoded; charset=UTF-8' \
   -d 'query=PREFIX+ldp:+<http://www.w3.org/ns/ldp#> SELECT+(COUNT(?s)+AS+?c)'\
   '+WHERE+{ ++?s+a+ldp:Resource+. ++?s+a+ldp:Container+. }+' \
   http://localhost:5000/query/sparql

Python API Retrieval
~~~~~~~~~~~~~~~~~~~~

In order to illustrate the advantages of the Python API, a sample retrieval of
the container resource after the load has been timed. This was done in an
IPython console::

   In [1]: from lakesuperior import env_setup
   In [2]: from lakesuperior.api import resource as rsrc_api
   In [3]: %timeit x = rsrc_api.get('/pomegranate').imr

Results
-------

.. _rather-snappy-laptop-1:

‘Rather Snappy’ Laptop
~~~~~~~~~~~~~~~~~~~~~~

10K Resources
^^^^^^^^^^^^^

=========================  ============  ============  ============  ============  ================
System                     PUT           Store         GET           SPARQL Query  Py-API retrieval
=========================  ============  ============  ============  ============  ================
FCREPO / Modeshape 4.7.5   49ms (100%)   3.7Gb (100%)  6.2s (100%)   N/A           N/A
Lakesuperior 1.0a17        78ms (159%)   298Mb (8%)    2.8s          0m1.194s      Not measured
Lakesuperior 1.0a18        62ms (126%)   789Mb (21%)   2.2s          0m2.214s      66ms
=========================  ============  ============  ============  ============  ================

**Notes:**

- The Python API time for the GET request in alpha18 is 8.5% of the request.
  This means that over 91% of the time is spent serializing the results.
  This time could be dramatically reduced by using faster serialization
  libraries, or can be outright zeroed out by an application that uses the
  Python API directly and manipulates the native RDFLib objects (of course, if
  a serialized output is eventually needed, that cost is unavoidable).
- Similarly, the ``triples`` retrieval method of the SPARQL query only takes
  13.6% of the request time. The rest is spent evaluating SPARQL and results.
  An application can use ``triples`` directly for relatively simple lookups
  without that overhead.

100K Resources
^^^^^^^^^^^^^^

=========================  ===============  =============  =============  ===============  ============  ================
System                     PUT              POST           Store          GET              Query         Py-API retrieval
=========================  ===============  =============  =============  ===============  ============  ================
FCREPO / Modeshape 4.7.5   500ms* (100%)    38ms (100%)    13Gb (100%)    2m6.7s (100%)    N/A           N/A
Lakesuperior 1.0a17        104ms (21%)      104ms (273%)   5.3Gb (40%)    0m17.0s (13%)    0m12.481s     3810ms
Lakesuperior 1.0a18        79ms (15%)       79ms  (207%)   7.5Gb (58%)    0m14.2s (11%)    0m4.214s**    905ms
=========================  ===============  =============  =============  ===============  ============  ================

\* POST was stopped at 50K resources. From looking at ingest timings over time
we can easily infer that ingest time would further increase. This is the
manifestation of the "many members" issue. The "Store" value is for the PUT
operation which ran regularly with 100K resources.

\*\* Timing based on a warm cache. The first query timed at 0m22.2s.

.. _ole-workhorse-server-1:

‘Ole Workhorse’ server
~~~~~~~~~~~~~~~~~~~~~~

10K Resources
^^^^^^^^^^^^^

=========================  ==============  ==============  ==============  ==============  ==================
System                     PUT             Store           GET             SPARQL Query    Py-API retrieval
=========================  ==============  ==============  ==============  ==============  ==================
FCREPO / Modeshape 4.7.5   285ms (100%)    3.7Gb (100%)    9.6s (100%)     N/A             N/A
Lakesuperior 1.0a17        446ms           298Mb           5.6s (58%)      0m1.194s        Not measured
Lakesuperior 1.0a18        Not measured    Not measured    Not measured    Not measured    Not measured
=========================  ==============  ==============  ==============  ==============  ==================

Conclusions
-----------

Lakesuperior appears to be markedly slower on writes and markedly faster
on reads. Both these factors are very likely related to the underlying
LMDB store which is optimized for read performance.

In a real-world application scenario, in which a client may perform multiple
reads before and after storing resources, the write performance gap may
decrease. A Python application using the Python API for querying and writing
would experience a dramatic improvement in reading timings, and somewhat in
write timings.

Comparison of results between the laptop and the server demonstrates
that both read and write performance ratios between repository systems are
identical in the two environments.

As it may be obvious, these are only very partial and specific
results. They should not be taken as a thorough performance assessment.
Such an assessment may be impossible and pointless to make given the
very different nature of the storage models, which may behave radically
differently depending on many variables.
