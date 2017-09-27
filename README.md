# LAKEsuperior

LAKEsuperior is an experimental [Fedora Repository](http://fedorarepository.org)
implementation.

## Basic concepts

LAKEsuperior stores LDP resources in a triplestore (currently Blazegraph) and
non-RDF sources (i.e. binaries) in a filesystem.

Resources are stored in discrete named graphs under the hood. A "main" graph
contains metadata about the resources, e.g. provenance information (NOTE:
versioning is not in the Level 1 scope).

[@TODO more]

## Installation

This is currently just a couple of config files, so there is nothing to install
yet. However, in the happiest possible outcome for Level 1, it should go like
this:

1. Install [Blazegraph](https://sourceforge.net/projects/bigdata/files/bigdata/)
2. Create a folder to store binary contents
3. Copy the `etc.skeleton` folder to a separate location
4. Configure the application and optionally add custom namespaces
5. Run `server.py`

## Status and development

LAKEsuperior is in **pre-alpha** status.

Development will be planned in subsequent "levels", the scope of each depending
on the outcomes of the previous level development and feedback:

- Level 1: proof of concept. This implementation will adhere to the current
Fedora Repository v4 specifications, in order to provide a testbed application
that easily integrates with Samvera and/or Islandora and can be used to test
and compare features with the official Fedora 4 implementation.
- Level 2: After a review with community members, the goal is to produce a beta
release that includes basic features to become a drop-in (or near there) with
the official Fedora 4.
- Level 3: production quality release.
- Level 4: Conform to the new [Fedora API specifications](http://fedora.info/spec/)

## Further docuentation

The design documents are in the `doc/pdf` folder.
