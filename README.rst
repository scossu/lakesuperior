LAKEsuperior
============

|build status| |docs|

LAKEsuperior is an alternative `Fedora
Repository <http://fedorarepository.org>`__ implementation.

Fedora is a mature repository software system historically adopted by
major cultural heritage institutions. It exposes an
`LDP <https://www.w3.org/TR/ldp-primer/>`__ endpoint to manage
any type of binary files and their metadata in Linked Data format.

The full documentation is maintained in `Read The Docs
<http://lakesuperior.readthedocs.io/>`__. Please refer to that for more info,
including installation instructions.

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
   container
-  Extensible provenance metadata tracking
-  Multi-modal access: HTTP (REST), command line interface and native Python
   API.
-  Fits in a pocket: you can carry 50M triples in an 8Gb memory stick.

.. |build status| image:: http://img.shields.io/travis/scossu/lakesuperior/master.svg?style=flat
   :alt: Build Status
   :target: https://travis-ci.org/username/repo

.. |docs| image:: https://readthedocs.org/projects/lakesuperior/badge/
    :alt: Documentation Status
    :target: https://lakesuperior.readthedocs.io/en/latest/?badge=latest
