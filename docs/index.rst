LAKEsuperior
============

|build status|

LAKEsuperior is an alternative `Fedora
Repository <http://fedorarepository.org>`__ implementation.

Fedora is a mature repository software system historically adopted by
major cultural heritage institutions. It exposes an
`LDP <https://www.w3.org/TR/ldp-primer/>`__ endpoint to manage
any type of binary files and their metadata in Linked Data format.

Guiding Principles
------------------

LAKEsuperior aims at being an uncomplicated, efficient Fedora 4
implementation.

Its main goals are:

-  **Reliability:** Based on solid technologies with stability in mind.
-  **Efficiency:** Small memory and CPU footprint, high scalability.
-  **Ease of management:** Tools to perform monitoring and maintenance
   included.
-  **Simplicity of design:** Straight-forward architecture, robustness
   over features.

Key features
------------

-  Drop-in replacement for Fedora4 (with some
   :doc:`caveats <fcrepo4_deltas>`); currently being tested
   with Hyrax 2
-  Very stable persistence layer based on
   `LMDB <https://symas.com/lmdb/>`__ and filesystem. Fully
   ACID-compliant writes guarantee consistency of data.
-  Term-based search (*planned*) and SPARQL Query API + UI
-  No performance penalty for storing many resources under the same
   container; no
   `kudzu <https://www.nature.org/ourinitiatives/urgentissues/land-conservation/forests/kudzu.xml>`__
   pairtree segmentation \ `1 <#f1>`__\ 
-  Extensible :doc:`provenance metadata <model>` tracking
-  :doc:`Multi-modal access <architecture>`: HTTP
   (REST), command line interface and native Python API.
-  Fits in a pocket: you can carry 50M triples in an 8Gb memory stick.

Implementation of the official `Fedora API
specs <https://fedora.info/spec/>`__ (Fedora 5.x and beyond) is not
foreseen in the short term, however it would be a natural evolution of
this project if it gains support.

Please make sure you read the :doc:`Delta
document <fcrepo4_deltas>` for divergences with the
official Fedora4 implementation.

Target Audience
---------------

LAKEsuperior is for anybody who cares about preserving data in the long
term.

Less vaguely, LAKEsuperior is targeted at who needs to store large
quantities of highly linked metadata and documents.

Its Python/C environment and API make it particularly well suited for
academic and scientific environments who would be able to embed it in a
Python application as a library or extend it via plug-ins.

LAKEsuperior is able to be exposed to the Web as a `Linked Data
Platform <https://www.w3.org/TR/ldp-primer/>`__ server. It also acts as
a SPARQL query (read-only) endpoint, however it is not meant to be used
as a full-fledged triplestore at the moment.

In its current status, LAKEsuperior is aimed at developers and hands-on
managers who are interested in evaluating this project.

Status and development
----------------------

LAKEsuperior is in **alpha** status. Please see the `project
issues <https://github.com/scossu/lakesuperior/issues>`__ list for a
rudimentary road map.

Contributing
------------

This has been so far a single person’s off-hours project (with much
input from several sides). In order to turn into anything close to a
Beta release and eventually to a production-ready implementation, it
needs some community love.

Contributions are welcome in all forms, including ideas, issue reports,
or even just spinning up the software and providing some feedback.
LAKEsuperior is meant to live as a community project.

--------------

1 However if your client splits pairtrees upstream, such as Hyrax does,
that obviously needs to change to get rid of the path segments.
`↩ <#a1>`__

.. |build status| image:: http://img.shields.io/travis/scossu/lakesuperior/master.svg?style=flat
   :target: https://travis-ci.org/username/repo

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :maxdepth: 2
   :caption: Contents

    Installation and Configuration <setup>
    Architecture Overview <architecture>
    Divergences from Fedora 4 <fcrepo4_deltas>
    Messaging <messaging>
    Migration Guide <migration>
    Command Line Reference <cli>
    Performance Benchmarks <performance>

.. toctree::
   :maxdepth: 1
   :caption: In-depth tech & design

    Contributing <contributing>
    API documentation <api>
    Indexing Strategy <indexing_strategy>
    Storage Implementation <storage>
    Content Model <model>
