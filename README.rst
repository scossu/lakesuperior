Lakesuperior
============

|build status| |docs| |pypi| |codecov|

Lakesuperior is a Linked Data repository software. It is capable of storing and
managing  large volumes of files and their metadata regardless of their
format, size, ethnicity, gender identity or expression.

Lakesuperior is an alternative `Fedora Repository
<http://fedorarepository.org>`__ implementation. Fedora is a mature repository
software system historically adopted by major cultural heritage institutions
which extends the `Linked Data Platform <https://www.w3.org/TR/ldp-primer/>`__
protocol.

Guiding Principles
------------------

Lakesuperior aims at being a reliable and efficient Fedora 4 implementation.

Its main goals are:

-  **Reliability:** Based on solid technologies with stability in mind.
-  **Efficiency:** Small memory and CPU footprint, high scalability.
-  **Ease of management:** Tools to perform migration, monitoring and
   maintenance included.
-  **Simplicity of design:** Straight-forward architecture, robustness
   over features.

Key features
------------

- Stores binary files and RDF metadata in one repository.
- Multi-modal access: REST/LDP, command line and native Python API.
- (`almost <fcrepo4_deltas>`_) Drop-in replacement for Fedora4
- Very stable persistence layer based on
  `LMDB <https://symas.com/lmdb/>`__ and filesystem. Fully
  ACID-compliant writes guarantee consistency of data.
- Term-based search and SPARQL Query API + UI
- No performance penalty for storing many resources under the same
  container, or having one resource link to many URIs
- Extensible provenance metadata tracking
- Fits in a pocket: you can carry 50M triples in an 8Gb memory stick.

Installation & Documentation
----------------------------

With Docker::

    git clone --recurse-submodules https://github.com/scossu/lakesuperior.git
    cd lakesuperior
    docker-compose up

With pip (requires a C compiler to be installed)::

    pip install lakesuperior

The full, current documentation is maintained in `Read The Docs
<http://lakesuperior.readthedocs.io/>`__. Please refer to that for more info,
including installation instructions.

.. |build status| image:: http://img.shields.io/travis/scossu/lakesuperior/master.svg?style=flat
   :alt: Build Status
   :target: https://travis-ci.org/username/repo

.. |docs| image:: https://readthedocs.org/projects/lakesuperior/badge/
    :alt: Documentation Status
    :target: https://lakesuperior.readthedocs.io/en/latest/?badge=latest

.. |pypi| image:: https://badge.fury.io/py/lakesuperior.svg
    :alt: PyPI Package
    :target: https://badge.fury.io/py/lakesuperior

.. |codecov| image:: https://codecov.io/gh/scossu/lakesuperior/branch/master/graph/badge.svg
  :alt: Code coverage
  :target: https://codecov.io/gh/scossu/lakesuperior

