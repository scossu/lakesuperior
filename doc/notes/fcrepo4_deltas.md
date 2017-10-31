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
`rest/8c9a074e-dda3-5256-ea30-eec2dd4fcf61` (obviously the identifiers will be
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

## Asynchronous processing

*TODO*

The server may reply with a 202 if the `Prefer` header is set to
`respond-async`.

