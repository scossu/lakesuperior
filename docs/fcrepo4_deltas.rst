Divergencies between lakesuperior and FCREPO4
=============================================

This is a (vastly incomplete) list of discrepancies between the current
FCREPO4 implementation and LAKEsuperior. More will be added as more
clients will use it.

Not yet implemented (but in the plans)
--------------------------------------

-  Various headers handling
-  Versioning (incomplete)
-  AuthN/Z
-  Fixity check
-  Blank nodes

Potentially breaking changes
----------------------------

The following divergences may lead into incompatibilities with some
clients.

Atomicity
~~~~~~~~~

FCREPO4 supports batch atomic operations whereas a transaction can be
opened and a number of operations (i.e. multiple R/W requests to the
repository) can be performed. The operations are persisted in the
repository only if and when the transaction is committed.

LAKesuperior only supports atomicity for a single HTTP request. I.e. a
single HTTTP request that should result in multiple write operations to
the storage layer is only persisted if no exception is thrown.
Otherwise, the operation is rolled back in order to prevent resources to
be left in an inconsistent state.

Tombstone methods
~~~~~~~~~~~~~~~~~

If a client requests a tombstone resource in FCREPO4 with a method other
than DELETE, the server will return ``405 Method Not Allowed``
regardless of whether the tombstone exists or not.

LAKEsuperior will return ``405`` only if the tombstone actually exists,
``404`` otherwise.

Web UI
~~~~~~

FCREPO4 includes a web UI for simple CRUD operations.

Such a UI is not in the immediate LAKEsuperior development plans.
However, a basic UI is available for read-only interaction: LDP resource
browsing, SPARQL query and other search facilities, and administrative
tools. Some of the latter *may* involve write operations, such as
clean-up tasks.

Automatic path segment generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``POST`` request without a slug in FCREPO4 results in a pairtree
consisting of several intermediate nodes leading to the automatically
minted identifier. E.g.

::

    POST /rest

results in ``/rest/8c/9a/07/4e/8c9a074e-dda3-5256-ea30-eec2dd4fcf61``
being created.

The same request in LAKEsuperior would create
``/rest/8c9a074e-dda3-5256-ea30-eec2dd4fcf61`` (obviously the
identifiers will be different).

This seems to break Hyrax at some point, but might have been fixed. This
needs to be verified further.

Allow PUT requests with empty body on existing resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FCREPO4 returns a ``409 Conflict`` if a PUT request with no payload is sent
to an existing resource.

LAKEsuperior allows to perform this operation, which would result in deleting
all the user-provided properties in that resource.

If the original resource is an LDP-NR, however, the operation will raise a
``415 Unsupported Media Type`` because the resource will be treated as an empty
LDP-RS, which cannot replace an existing LDP-NR.

Non-standard client breaking changes
------------------------------------

The following changes may be incompatible with clients relying on some
FCREPO4 behavior not endorsed by LDP or other specifications.

Pairtrees
~~~~~~~~~

FCREPO4 generates “pairtree” resources if a resource is created in a
path whose segments are missing. E.g. when creating ``/a/b/c/d``, if
``/a/b`` and ``/a/b/c`` do not exist, FCREPO4 will create two Pairtree
resources. POSTing and PUTting into Pairtrees is not allowed. Also, a
containment triple is established between the closest LDPC and the
created resource, e.g. if ``a`` exists, a
``</a> ldp:contains </a/b/c/d>`` triple is created.

LAKEsuperior does not employ Pairtrees. In the example above
LAKEsuperior would create a fully qualified LDPC for each missing
segment, which can be POSTed and PUT to. Containment triples are created
between each link in the path, i.e. ``</a> ldp:contains </a/b>``,
``</a/b> ldp:contains </a/b/c>`` etc. This may potentially break clients
relying on the direct containment model.

The rationale behind this change is that Pairtrees are the byproduct of
a limitation imposed by Modeshape and introduce complexity in the
software stack and confusion for the client. LAKEsuperior aligns with
the more intuitive UNIX filesystem model, where each segment of a path
is a “folder” or container (except for the leaf nodes that can be either
folders or files). In any case, clients are discouraged from generating
deep paths in LAKEsuperior without a specific purpose because these
resources create unnecessary data.

Non-mandatory, non-authoritative slug in version POST
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FCREPO4 requires a ``Slug`` header to POST to ``fcr:versions`` to create
a new version.

LAKEsuperior adheres to the more general FCREPO POST rule and if no slug
is provided, an automatic ID is generated instead. The ID is a UUID4.

Note that internally this ID is not called “label” but “uid” since it is
treated as a fully qualified identifier. The ``fcrepo:hasVersionLabel``
predicate, however ambiguous in this context, will be kept until the
adoption of Memento, which will change the retrieval mechanisms.

Another notable difference is that if a POST is issued on the same resource
``fcr:versions`` location using a version ID that already exists, LAKEsuperior
will just mint a random identifier rather than returning an error.

Deprecation track
-----------------

LAKEsuperior offers some “legacy” options to replicate the FCREPO4
behavior, however encourages new development to use a different approach
for some types of interaction.

Endpoints
~~~~~~~~~

The FCREPO root endpoint is ``/rest``. The LAKEsuperior root endpoint is
``/ldp``.

This should not pose a problem if a client does not have ``rest``
hard-coded in its code, but in any event, the ``/rest`` endpoint is
provided for backwards compatibility.

Future implementations of the Fedora API specs may employ a "versioned"
endpoint scheme that allows multiple Fedora API versions to be available to the
client, e.g. ``/ldp/fc4`` for the current LDP API version, ``/ldp/fc5`` for
Fedora version 5.x, etc.

Automatic LDP class assignment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since LAKEsuperior rejects client-provided server-managed triples, and
since the LDP types are among them, the LDP container type is inferred
from the provided properties: if the ``ldp:hasMemberRelation`` and
``ldp:membershipResource`` properties are provided, the resource is a
Direct Container. If in addition to these the
``ldp:insertedContentRelation`` property is present, the resource is an
Indirect Container. If any of the first two are missing, the resource is
a Container.

Clients are encouraged to omit LDP types in PUT, POST and PATCH
requests.

Lenient handling
~~~~~~~~~~~~~~~~

FCREPO4 requires server-managed triples to be expressly indicated in a
PUT request, unless the ``Prefer`` header is set to
``handling=lenient; received="minimal"``, in which case the RDF payload
must not have any server-managed triples.

LAKEsuperior works under the assumption that client should never provide
server-managed triples. It automatically handles PUT requests sent to
existing resources by returning a 412 if any server managed triples are
included in the payload. This is the same as setting ``Prefer`` to
``handling=strict``, which is the default.

If ``Prefer`` is set to ``handling=lenient``, all server-managed triples
sent with the payload are ignored.

Clients using the ``Prefer`` header to control PUT behavior as
advertised by the specs should not notice any difference.

Optional improvements
---------------------

The following are improvements in performance or usability that can only
be taken advantage of if client code is adjusted.

LDP-NR content and metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~

FCREPO4 relies on the ``/fcr:metadata`` identifier to retrieve RDF
metadata about an LDP-NR. LAKEsuperior supports this as a legacy option,
but encourages the use of content negotiation to do the same while
offering explicit endpoints for RDF and non-RDF content retrieval.

Any request to an LDP-NR with an ``Accept`` header set to one of the
supported RDF serialization formats will yield the RDF metadata of the
resource instead of the binary contents.

The ``fcr:metadata`` URI returns the RDF metadata of a LDP-NR.

The ``fcr:content`` URI returns the non-RDF content.

The two optionsabove return an HTTP error if requested for a LDP-RS.

“Include” and “Omit” options for children
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LAKEsuperior offers an additional ``Prefer`` header option to exclude
all references to child resources (i.e. by removing all the
``ldp:contains`` triples) while leaving the other server-managed triples
when retrieving a resource:

::

    Prefer: return=representation; [include | omit]="http://fedora.info/definitions/v4/repository#Children"

The default behavior is to include all children URIs.

Soft-delete and purge
~~~~~~~~~~~~~~~~~~~~~

**NOTE**: The implementation of this section is incomplete and debated.

In FCREPO4 a deleted resource leaves a tombstone deleting all traces of
the previous resource.

In LAKEsuperior, a normal DELETE creates a new version snapshot of the
resource and puts a tombstone in its place. The resource versions are
still available in the ``fcr:versions`` location. The resource can be
“resurrected” by issuing a POST to its tombstone. This will result in a
``201``.

If a tombstone is deleted, the resource and its versions are completely
deleted (purged).

Moreover, setting the ``Prefer:no-tombstone`` header option on DELETE
allows to delete a resource and its versions directly without leaving a
tombstone.
