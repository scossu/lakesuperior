About Lakesuperior
==================

Lakesuperior is a repository system that stores binary files and their metadata
as Linked Data. It is a `Fedora Repository <http://fedorarepository.org>`__
implementation focused on efficiency, stability and integration with Python.

Fedora is a mature repository software system historically adopted by
major cultural heritage institutions. It exposes an
`LDP <https://www.w3.org/TR/ldp-primer/>`__ endpoint to manage
any type of binary files and their metadata in Linked Data format.

Guiding Principles
------------------

Lakesuperior aims at being an efficient and flexible Fedora 4 implementation.

Its main goals are:

-  **Reliability:** Based on solid technologies with stability in mind.
-  **Efficiency:** Small memory and CPU footprint, high scalability.
-  **Ease of management:** Tools to perform monitoring and maintenance
   included.
-  **Simplicity of design:** Straight-forward architecture, robustness
   over features.

Key features
------------

-  Drop-in replacement for Fedora4 (with some :doc:`caveats <fcrepo4_deltas>`)
-  Very stable persistence layer based on
   `LMDB <https://symas.com/lmdb/>`__ and filesystem. Fully
   ACID-compliant writes guarantee consistency of data.
-  Term-based search and SPARQL Query API + UI
-  No performance penalty for storing many resources under the same
   container; no `kudzu
   <https://www.nature.org/ourinitiatives/urgentissues/land-conservation/forests/kudzu.xml>`__
   pairtree segmentation.
-  Extensible :doc:`provenance metadata <model>` tracking
-  :doc:`Multi-modal access <architecture>`: HTTP
   (REST), command line interface and native Python API.
-  Fits in a pocket: you can carry 64M triples in a 32Gb memory stick [#]_.

Implementation of the official `Fedora API
specs <https://fedora.info/spec/>`__ and OCFL are currently being
considered as the next major development steps.

Please make sure you read the :doc:`Delta document <fcrepo4_deltas>` for
divergences with the official Fedora4 implementation.

Target Audience
---------------

Lakesuperior is for anybody who cares about preserving data in the long
term.

Less vaguely, Lakesuperior is targeted at who needs to store large
quantities of highly linked metadata and documents.

Its Python/C environment and API make it particularly well suited for
academic and scientific environments who would be able to embed it in a
Python application as a library or extend it via plug-ins.

Lakesuperior is able to be exposed to the Web as a `Linked Data
Platform <https://www.w3.org/TR/ldp-primer/>`__ server. It also acts as
a SPARQL query (read-only) endpoint, however it is not meant to be used
as a full-fledged triplestore at the moment.

In its current status, Lakesuperior is aimed at developers and hands-on
managers who are interested in evaluating this project.

Status and development
----------------------

Lakesuperior is in **alpha** status. Please see the `project
issues <https://github.com/scossu/lakesuperior/issues>`__ list for a
rudimentary road map.

Acknowledgements & Caveat
-------------------------

Most of this code has been written on the Chicago CTA Blue Line train and, more
recently, on the Los Angeles Metro 734 bus. The author would like to thank
these companies for providing an office on wheels for this project.

Potholes on Sepulveda street may have caused bugs and incorrect documentation.
Please report them if you find any.

-------------------

.. [#] Your mileage may vary depending on the variety of your triples.
