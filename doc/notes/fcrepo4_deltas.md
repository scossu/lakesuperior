# Divergencies between lakesuperior and FCREPO4

This is a (vastly incomplete) list of discrepancies between the current FCREPO4
implementation and LAKEsuperior. More will be added as more clients will use
it.


## Not yet implemented (but in the plans)

See [TODO](TODO)

- Various header handling
- Versioning
- AuthN/Z
- Fixity check
- Blank nodes
- Hash URIs


## Potentially breaking changes

The following  divergences may lead into incompatibilities with some clients.

### Atomicity

FCREPO4 supports batch atomic operations whereas a transaction can be opened
and a number of operations (i.e. multiple R/W requests to the repository) can
be performed. The operations are persisted in the repository only if and when
the transaction is committed.

LAKesuperior only supports atomicity for a single LDP request. I.e. a single
HTTTP request that should reult in multiple write operations to the storage
layer is only persisted if no exception is thrown. Otherwise, the operation is
rolled back in order to prevent resources to be left in an inconsistent state.

### Tombstone methods

If a client requests a tombstone resource in
FCREPO4 with a method other than DELETE, the server will return `405 Method Not
Allowed` regardless of whether the tombstone exists or not.

LAKEsuperior will return `405` only if the tombstone actually exists, `404`
otherwise.

### Web UI

FCREPO4 includes a web UI for simple CRUD operations.

Such a UI is not in the immediate LAKEsuperior development plans. However, a very
basic UI may at some point be built for read-only interaction (i.e.
requesting resources and clicking through relationship links). A more complete
UI should be built for simple and/or SPARQL queries.


## Non-standard client breaking changes

The following changes may be incompatible with clients relying on some FCREPO4
behavior not endorsed by LDP or other specifications.

### Automatic pairtree generation

A `POST` request without a slug in FCREPO4 results in a pairtree consisting of
several intermediate nodes leading to the automatically minted identifier. E.g.

    POST /rest

results in `/rest/8c/9a/07/4e/8c9a074e-dda3-5256-ea30-eec2dd4fcf61` being
created.

The same request in LAKEsuperior would create
`/rest/8c9a074e-dda3-5256-ea30-eec2dd4fcf61` (obviously the identifiers will be
different).

### Explicit intermediate paths

In FCREPO4, a PUT request to `/rest/a/b/c`, given `/rest/a` and `rest/a/b` not
previously existing, results in the creation of Pairtree resources that are
retrievable. In LAKEsuperior the same operation results only in the creation of
containment triple in the graph store, which are not exposed in the LDP API.
Therefore, a GET to `rest/a` in FCREPO4 will result in a 200, a GET to `rest/a`
in LAKEsuperior in a 404.

In both above cases, PUTting into `rest/a` yields a 409, POSTing to it results
in a 201.

### Non-mandatory slug in version POST

FCREPO requires a `Slug` header to POST to `fcr:versions` to create a new
version.

LAKEsuperior adheres to the more general FCREPO POST rule and if no slug is
provided, an automatic ID is generated instead. The ID is a UUID4.

Also, note that internally this ID is not called "label" but "uid" since it is
treated as a fully qualified identifier. The `fcrepo:hasVersionLabel` predicate
has been kept as not worth changing until the adoption of Memento, which will
change the retrieval mechanisms.


## Deprecation track

LAKEsuperior offers some "legacy" options to replicate the FCREPO4 behavior,
however encourages new development to use a different approach for some types
of interaction.

### Endpoints

The FCREPO root endpoint is `/rest`. The LAKEsuperior root endpoint is `/ldp`.

This should not pose a problem if a client does not have `rest` hard-coded in
its code, but in any event, the `/rest` endpoint is provided for backwards
compatibility.

LAKEsuperior adds the (currently stub) `query` endpoint. Other endpoints for
non-LDP services may be opened in the future.

### Automatic LDP class assignment

Since LAKEsuperior rejects client-provided server-managed triples, and since
the LDP types are among them, the LDP container type is inferred from the
provided properties: if the `ldp:hasMemberRelation` and
`ldp:membershipResource` properties are provided, the resource is a Direct
Container. If in addition to these the `ldp:insertedContentRelation` property
is present, the resource is an Indirect Container. If any of the first two are
missing, the resource is a Container (@TODO discuss: shall it be a Basic
Container?)

Clients are encouraged to omit LDP types in PUT, POST and PATCH requests.

### Lenient handling

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

Clients using the `Prefer` header to control PUT behavior as advertised by the
specs should not notice any difference.

### LDP-NR metadata by content negotiation

FCREPO4 relies on the `/fcr:metadata` identifier to retrieve RDF metadata about
an LDP-NR. LAKEsuperior supports this as a legacy option, but encourages the
use of content negotiation to do the same. Any request to an LDP-NR with an
`Accept` header set to one of the supported RDF serialization formats will
yield the RDF metadata of the resource instead of the binary contents.


## Optional improvements

The following are improvements in performance or usability that can only taken
advantage of if client code is adjusted.

### "Include" and "Omit" options for children

LAKEsuperior offers an additional `Prefer` header option to exclude all
references to child resources (i.e. by removing all the `ldp:contains` triples)
while leaving the other server-managed triples when retrieving a resource:

    Prefer: return=representation; [include | omit]="http://fedora.info/definitions/v4/repository#Children"

The default behavior is including all children URIs.

### Soft-delete and purge

In FCREPO4 a deleted resource leaves a tombstone deleting all traces of the
previous resource.

In LAKEsuperior, a normal DELETE creates a new version snapshot of the resource
and puts a tombstone in its place. The resource versions are still available
in the `fcr:versions` location. The resource can be "resurrected" by
issuing a POST to its tombstone. This will result in a `201`.

If a tombstone is deleted, the resource and its versions are completely deleted
(purged).

Moreover, setting the `Prefer:no-tombstone` header option on DELETE allows to
delete a resource and its versions directly without leaving a tombstone.
