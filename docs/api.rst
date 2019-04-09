API Documentation
==================

Main Interface
--------------

The Lakesuperior API modules of most interest for a client are:

- :mod:`lakesuperior.api.resource`
- :mod:`lakesupeiror.api.query`
- :mod:`lakesuperior.api.admin`

Lower-Level Interfaces
----------------------

:mod:`lakesuperior.model.ldp` handles the concepts of LDP resources,
containers, binaries, etc.

:mod:`lakesuperior.store.ldp_rs.rsrc_centric_layout` handles the "layout" of
LDP resources as named graphs in a triplestore. It is possible (currently not
without changes to the core libraries) to devise a different layout for e.g. a
more sparse, or richer, data model.

Similarly, :mod:`lakesuperior.store.ldp_nr.base_non_rdf_layout` offers an
interface to handle the layout of LDPR resources. Currently only one
implementation is available but it is also possible to create a new module to
e.g. handle files in an S3 bucket, a Cassandra database, or create Bagit or
OCFL file structures, and configure Lakesuperior to use one, or more, of those
persistence methods.

Deep Tissue
-----------

Some of the Cython libraries in :mod:`lakesuperior.model.structures`,
:mod:`lakesuperior.model.rdf`, and :mod:`lakesuperior.store` have
Python-accessible methods for high-performance manipulation. The
:py:class:`lakesuperior.model.rdf.graph.Graph` class is an example of that.

Full API Documentation
----------------------

.. toctree::
   :caption: Modules

   apidoc/modules
