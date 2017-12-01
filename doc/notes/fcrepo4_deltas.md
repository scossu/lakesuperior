# Divergencies between lakesuperior and FCREPO4

## Endpoints

The FCREPO root endpoint is `/rest`. The LAKEsuperior root endpoint is `/ldp`.

This should not pose a problem if a client does not have `rest` hard-coded in
its code, but in any event, the `/rest` endpoint is provided for backwards
compatibility.

LAKEsuperior adds the (currently stub) `query` endpoint. Other endpoints for
non-LDP services may be opened in the future.

## Automatic pairtree generation

A `POST` request without a slug in FCREPO4 results in a pairtree consisting of
several intermediate nodes leading to the automatically minted identifier. E.g.

~~~
POST /rest
~~~

results in `/rest/8c/9a/07/4e/8c9a074e-dda3-5256-ea30-eec2dd4fcf61` being
created.

The same request in LAKEsuperior would create
`/rest/8c9a074e-dda3-5256-ea30-eec2dd4fcf61` (obviously the identifiers will be
different).

## Explicit intermediate paths

In FCREPO4, a PUT request to `/rest/a/b/c`, given `/rest/a` and `rest/a/b` not
previously existing, results in the creation of Pairtree resources that are
retrievable. In LAKEsuperior the same operation results only in the creation of
containment triple in the graph store, which are not exposed in the LDP API.
Therefore, a GET to `rest/a` in FCREPO4 will result in a 200, a GET to `rest/a`
in LAKEsuperior in a 404.

In both above cases, PUTting into `rest/a` yields a 409, POSTing to it results
in a 201.

## Lenient handling

FCREPO4 requires server-managed triples to be expressly indicated in a PUT
request, unless the `Prefer` heeader is set to
`handling=lenient; received="minimal"`, in which case the RDF payload must not
have any server-managed triples.

LAKEsuperior works under the assumption that client should never provide
server-managed triples. It automatically handles PUT requests sent to existing
resources by returning a 412 if any server managed triples are included in the
payload. This is the same as setting `Prefer` to `handling=strict`, which is
the default.

If `Prefer` is set to `handling=lenient`, all server-managed triples sent with
the payload are ignored.

## "Include" and "Omit" options for children

LAKEsuperior offers an additional `Prefer` header option to exclude all
references to child resources (i.e. by removing all the `ldp:contains` triples)
while leaving the other server-managed triples when retrieving a resource:

    Prefer: return=representation; [include | omit]="http://fedora.info/definitions/v4/repository#Children"

The default behavior is including all children URIs.

## Automatic LDP class assignment

Since LAKEsuperior rejects client-provided server-managed triples, and since
the LDP types are among them, the LDP container type is inferred from the
provided properties: if the `ldp:hasMemberRelation` and
`ldp:membershipResource` properties are provided, the resource is a Direct
Container. If in addition to these the `ldp:insertedContentRelation` property
is present, the resource is an Indirect Container. If any of the first two are
missing, the resource is a Container (@TODO discuss: shall it be a Basic
Container?)

## LDP-NR metadata by content negotiation

FCREPO4 relies on the `/fcr:metadata` identifier to retrieve RDF metadata about
an LDP-NR. LAKEsuperior supports this as a legacy option, but encourages the
use of content negotiation to do the same. Any request to an LDP-NR with an
`Accept` header set to one of the supported RDF serialization formats will
yield the RDF metadata of the resource instead of the binary contents.

## Tombstone methods

If a client requests a tombstone resource in
FCREPO4 with a method other than DELETE, the server will return `405 Method Not
Allowed` regardless of whether the tombstone exists or not.

LAKEsuperior will return `405` only if the tombstone actually exists, `404`
otherwise.

## Atomicity

FCREPO4 supports batch atomic operations whereas a transaction can be opened
and a number of operations (i.e. multiple R/W requests to the repository) can
be performed. The operations are persisted in the repository only if and when
the transaction is committed.

LAKesuperior only supports atomicity for a single LDP request. I.e. a single
HTTTP request that should reult in multiple write operations to the storage
layer is only persisted if no exception is thrown. Otherwise, the operation is
rolled back in order to prevent resources to be left in an inconsistent state.

## Web UI

FCREPO4 includes a web UI for simple CRUD operations.

Such a UI is not foreseen to be built in LAKEsuperior any time soon since the
API interaction leaves a greater degree of flexibility. In addition, the
underlying triplestore layer may provide a UI for complex RDF queries.

