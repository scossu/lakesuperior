# LAKEsuperior

LAKEsuperior is an experimental [Fedora Repository](http://fedorarepository.org)
implementation.

## Guiding Principles

LAKEsuperior aims at being an uncomplicated, efficient Fedora 4 implementation.

Its main goals are:

- **Reliability:** Based on solid technologies with stability in mind.
- **Efficiency:** Small memory and CPU footprint, high scalability.
- **Ease of management:** Tools to perform monitoring and maintenance included.
- **Simplicity of design:** Straight-forward architecture, robustness over
  features.

## Key features

- Drop-in replacement for Fedora4 (with some
  [caveats](doc/notes/fcrepo4_deltas.md)); currently being tested with Hyrax 2
- Very stable persistence layer based on [LMDB](https://symas.com/lmdb/) and
  filesystem. Fully ACID-compliant writes guarantee consistency of data.
- Term-based search (*planned*) and SPARQL Query API + UI
- No performance penalty for storing many resources under the same container; no
  [kudzu](https://www.nature.org/ourinitiatives/urgentissues/land-conservation/forests/kudzu.xml)
  pairtree segmentation <sup id="a1">[1](#f1)</sup>
- Extensible [provenance metadata](doc/notes/model.md) tracking
- [Multi-modal access](doc/notes/architecture.md#multi-modal-access): HTTP
  (REST), command line interface and native Python API.
- Fits in a pocket: you can carry 50M triples in an 8Gb memory stick.

Implementation of the official [Fedora API specs](https://fedora.info/spec/)
(Fedora 5.x and beyond) is not
foreseen in the short term, however it would be a natural evolution of this
project if it gains support.

Please make sure you read the [Delta document](doc/notes/fcrepo4_deltas.md) for
divergences with the official Fedora4 implementation.

## Target Audience

LAKEsuperior is for anybody who cares about preserving data in the long term.

Less vaguely, LAKEsuperior is targeted at who needs to store large quantities
of highly linked metadata and documents.

Its Python/C environment and API make it particularly well suited for academic
and scientific environments who would be able to embed it in a Python
application as a library or extend it via plug-ins.

LAKEsuperior is able to be exposed to the Web as a
[Linked Data Platform](https://www.w3.org/TR/ldp-primer/) server. It also acts
as a SPARQL query (read-only) endpoint, however it is not meant to be used as
a full-fledged triplestore at the moment.

In its current status, LAKEsuperior is aimed at developers and
hands-on managers who are able to run a Python environment and are
interested in evaluating this project.

## Installation

### Dependencies

1. Python 3.5 or greater.
1. The [LMDB](https://symas.com/lmdb/) database library. It should be included
in most Linux distributions' standard package repositories.
1. A message broker supporting the STOMP protocol. For testing and evaluation
purposes, [Coilmq](https://github.com/hozn/coilmq) is included with the
dependencies and should be automatically installed.

### Installation steps

1. Install dependencies as indicated above
1. Create a virtualenv in a project folder:
   `virtualenv -p <python 3.5+ exec path> <virtualenv folder>`
1. Activate the virtualenv: `source <path_to_virtualenv>/bin/activate`
1. Clone this repo
1. `cd` into repo folder
1. Install dependencies: `pip install -r requirements.txt`
1. Copy the `etc.skeleton` folder to a separate location
1. Set the configuration folder location in the environment:
   `export FCREPO_CONFIG_DIR=<your config dir location>` (you can
   add this line at the end of your virtualenv `activate` script)
1. Configure the application if needed. The default settings should be fine
   for evaluation.
1. Start your STOMP broker, e.g.: `coilmq &`
1. Run `./lsup_admin bootstrap` to initialize the binary and graph stores
1. Run `./fcrepo`.

### Production deployment

If you like fried repositories for lunch, deploy before 11AM.

## Status and development

LAKEsuperior is in **alpha** status. Please see the
[project issues](https://github.com/scossu/lakesuperior/issues) list for a
rudimentary road map.

## Contributing

This has been so far a single person's off-hours project (with much input from
several sides). In order to turn into anything close to a Beta release and
eventually to a production-ready implementation, it needs some community love.

Contributions are welcome in all forms, including ideas, issue reports, or
even just spinning up the software and providing some feedback. LAKEsuperior is
meant to live as a community project.

## Technical documentation

[Architecture Overview](doc/notes/architecture.md)

[Content Model](doc/notes/model.md)

[Command-Line Reference](doc/notes/cli.md)

[Storage Implementation](doc/notes/storage.md)

[Performance Benchmarks](doc/notes/performance.md)

---

<b id="f1">1</b> However if your client splits pairtrees upstream, such as
Hyrax does, that obviously needs to change to get rid of the path
segments. [↩](#a1)
