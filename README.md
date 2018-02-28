# LAKEsuperior

LAKEsuperior is an experimental [Fedora Repository](http://fedorarepository.org)
implementation.

## Guiding Principles

LAKEsuperior aims at being an uncomplicated, efficient Fedora 4 implementation.

Its main goals are:

- *Simplicity of design:* LAKEsuperior relies on [LMDB](https://symas.com/lmdb/),
an embedded, high-performance key-value store, for storing metadata and on
the filesystem to store binaries.
- *Efficiency:* while raw speed is important, LAKEsuperior also aims at being
conservative with resources. Its memory and CPU footprint are small. Python C
extensions are used where possible to improve performance.
- *Reliability:* fully ACID-compliant writes guarantee consistency of data.
- *Ease of management:* Contents can be queried directly via term search or
SPARQL without the aid of external indices. Scripts and interfaces for
repository administration and monitoring are shipped with the standard release.
- *Portability:* aims at maintaining a minimal set of dependencies.

## Key features

- Drop-in replacement for Fedora4 (with some caveats: see
  [Delta document](doc/notes/fcrepo4_deltas.md))—currently being tested with
  Hyrax 2
- Term-based search (*planned*) and SPARQL Query API + UI
- No performance penalty for storing many resources under the same container; no
  [kudzu](https://www.nature.org/ourinitiatives/urgentissues/land-conservation/forests/kudzu.xml)
  pairtree segmentation <sup id="a1">[1](#f1)</sup>
- Constant performance writing to a resource with
  many children or members; option to omit children in retrieval
- Migration tools (*planned*)
- Python API (*planned*): Authors of Python clients can use LAKEsuperior as an
  embedded repository with no HTTP traffic or interim RDF serialization &
  de-serialization involved.
- Fits in a pocket: you can carry over 50M triples in an 8Gb memory stick.

Implementation of the official [Fedora API specs](https://fedora.info/spec/)
(Fedora 5.x and beyond) is not
foreseen in the short term, however it would be a natural evolution of this
project if it gains support.

Please make sure you read the [Delta document](doc/notes/fcrepo4_deltas.md) for
divergences with the official Fedora4 implementation.

## Installation

### Dependencies

1. Python 3.5 or greater.
1. The [LMDB](https://symas.com/lmdb/) database library. It should be included
in most Linux distributions' standard package repositories.
1. A message broker supporting the STOMP protocol. For testing and evaluation
purposes, Coilmq is included in the dependencies and should be automatically
installed.

### Installation steps

1. Install dependencies as indicated above
1. Create a virtualenv in a project folder:
   `virtualenv -p <python 3.5+ exec path> <virtualenv folder>`
1. Initialize the virtualenv: `source <path_to_virtualenv>/bin/activate`
1. Clone this repo
1. `cd` into repo folder
1. Install dependencies: `pip install -r requirements.txt`
1. Copy the `etc.skeleton` folder to a separate location
1. Set the configuration folder location in the environment:
   `export FCREPO_CONFIG_DIR=<your config dir location>` (alternatively you can
   add this line to your virtualenv `activate` script)
1. Configure the application
1. Start your STOMP broker
1. Run `util/bootstrap.py` to initialize the binary and graph stores
1. Run `./fcrepo` for a multi-threaded server or `flask run` for a
   single-threaded development server

### Production deployment

If you like fried repositories for lunch, deploy before 11AM.

## Status and development

LAKEsuperior is in **alpha** status. Please see the [TODO](doc/notes/TODO) list
for a rudimentary road map and status.

## Technical documentation

[Storage Implementation](doc/notes/torage.md)

[Performance benchmarks](doc/notes/performance.md)

[TODO list](doc/notes/TODO)

---

<b id="f1">1</b> However if your client splits pairtrees upstream, such as
Hyrax does, that obviously needs to change to get rid of the path
segments. [↩](#a1)
