Performance Benchmark Report
============================

The purpose of this document is to provide very broad performance measurements
and comparison between Lakesuperior and Fedora/Modeshape implementations.

Environment
-----------

Hardware
~~~~~~~~

-  MacBook Pro14,2
-  1x Intel(R) Core(TM) i5 @3.1Ghz
-  16Gb RAM
-  SSD
-  OS X 10.13
-  python 3.7.2
-  lmdb 0.9.22

Benchmark script
~~~~~~~~~~~~~~~~

`Generator script <../../util/benchmark.py>`__

The script was run with default values: resprectively 10,000 and 100,000
children under the same parent. PUT and POST requests were tested separately.

The script calculates only the timings used for the PUT or POST requests, not
counting the time used to generate the random data.

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
   In [3]: %timeit x = rsrc_api.get('/pomegranate').imr.as_rdflib

Results
-------

10K Resources
^^^^^^^^^^^^^

===============================  =============  =============  ============  ============  ============
System                           PUT            POST           Store         GET           SPARQL Query
===============================  =============  =============  ============  ============  ============
FCREPO / Modeshape 4.7.5         68ms (100%)    XXms (100%)    3.9Gb (100%)  6.2s (100%)   N/A         
Lakesuperior 1.0a20 REST API     105ms (159%)   XXXms (XXX%)   298Mb (8%)    2.1s          XXXXXXXs    
Lakesuperior 1.0a20 Python API   53ms (126%)    XXms (XXX%)    789Mb (21%)   381ms         N/A         
===============================  =============  =============  ============  ============  ============

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

===============================  ===============  ===============  =============  ===============  ==============
System                           PUT              POST             Store          GET              SPARQL Query  
===============================  ===============  ===============  =============  ===============  ==============
FCREPO / Modeshape 4.7.5         500+ms*          65ms (100%)\*\*  12Gb (100%)    3m41s (100%)     N/A           
Lakesuperior 1.0a20 REST API     104ms (100%)     123ms (189%)     8.7Gb (72%)    30s (14%)        19.3s (100%)  
Lakesuperior 1.0a20 Python API   69ms (60%)       58ms (89%)       8.7Gb (72%)    6s (2.7%)        9.17s (47%)
===============================  ===============  ===============  =============  ===============  ==============

\* POST was stopped at 30K resources after the ingest time reached >1s per
resource. This is the manifestation of the "many members" issue which is
visible in the graph below. The "Store" value is for the PUT operation which
ran regularly with 100K resources.

\*\* the POST test with 100K resources was conducted with fedora 4.7.5 because
5.0 would not automatically create a pairtree, thereby resulting in the same
performance as the PUT method.

\*\*\* Timing based on a warm cache. The first query timed at 0m22.2s.

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
