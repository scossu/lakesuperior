# LAKEsuperior Content Model Rationale

## Internal and Public URIs; Identifiers

Resource URIs are stored internally in LAKEsuperior as domain-agnostic URIs
with the scheme `info:fcres<resource UID>`. This allows resources to be
portable across systems. E.g. a resource with an internal URI of
`info:fcres/a/b/c`, when accessed via the `http://localhost:8000/ldp`
endpoint, will be found at `http://localhost:8000/ldp/a/b/c`.

The resource UID making up the looks like a UNIX
filesystem path, i.e. it always starts with a forward slash and can be made up
of multiple segments separated by slashes. E.g. `/` is the root node UID,
`/a` is a resource UID just below root. their internal URIs are `info:fcres/`
and `info:fcres/a` respectively.

In the Python API, the UID and internal URI of an LDP resource can be accessed
via the `uid` and `uri` properties respectively:

```
>>> import lakesuperior.env_setup
>>> from lakesuperior.api import resource
>>> rsrc = resource.get('/a/b/c')
>>> rsrc.uid
/a/b/c
>>> rsrc.uri
rdflib.terms.URIRef('info:fcres/a/b/c')
```

## Store Layout

One of the key concepts in LAKEsuperior is the store layout. This is a
module built with a
specific purpose in mind, i.e. allowing fine-grained recording of provenance
metadata while providing reasonable performance.

Store layout modules could be replaceable (work needs to
be done to develop an interface to allow that). The default (and only at the
moment) layout shipped with LAKEsuperior is the
[resource-centric layout](../../lakesuperior/store/ldp_rs/rsrc_centric_layout).
This layout implements a so-called
[graph-per-aspect pattern](http://patterns.dataincubator.org/book/graph-per-aspect.html)
which stores different sets of statements about a resource in separate named
graphs.

The named graphs used for each resource are:

- An admin graph (`info:fcsystem/graph/admin<resource UID>`) which stores
  administrative metadata, mostly server-managed triples such as LDP types,
  system create/update timestamps and agents, etc.
- A structure graph (`info:fcsystem/graph/structure<resource UID>`) reserved for
  containment triples. The reason
  for this separation is purely convenience, since it makes it easy to retrieve
  all the properties of a large container without its child references.
- One (and, possibly, in the future, more user-defined) named graph for
  user-provided data (`info:fcsystem/graph/userdata/_main<resource UID>`).

Each of these graphs can be annotated with provenance metadata. The layout
decides which triples go in which graph based on the predicate or RDF type
contained in the triple. Adding logic to support arbitrary named graphs based
e.g. on user agent, or to add more provenance information, should be relatively
simple.

