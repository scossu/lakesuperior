Lakesuperior Architecture
=========================

Lakesuperior is written in Python. It is not excluded that parts of the
code may be rewritten in `Cython <http://cython.readthedocs.io/>`__ for
performance.

Multi-Modal Access
------------------

Lakesuperior services and data are accessible in multiple ways:

-  Via HTTP. This is the canonical way to interact with LDP resources
   and conforms quite closely to the Fedora specs (currently v4).
-  Via command line. This method includes long-running admin tasks which
   are not available via HTTP.
-  Via a Python API. This method allows to use Python scripts to access
   the same methods available to the two methods above in a programmatic
   way. It is possible to write Python plugins or even to embed
   Lakesuperior in a Python application, even without running a web
   server.

Architecture Overview
---------------------

.. figure:: assets/lakesuperior_arch.png
   :alt: Lakesuperior Architecture

   Lakesuperior Architecture

The Lakesuperior REST API provides access to the underlying Python API.
All REST and CLI operations can be replicated by a Python program
accessing this API.

The main advantage of the Python API is that it makes it very easy to
maipulate graph and binary data without the need to serialize or
deserialize native data structures. This matters when handling large ETL
jobs for example.

The Python API is divided in three main areas:

-  Resource API: this API in charge of all the resource CRUD operations and
   implements the majority of the Fedora specs.
-  Admin API: exposes utility methods, mostly long-running maintenance jobs.
-  Query API: provides several facilities for querying repository data.


See :doc:`API documentation<api>` for more details.
