Sample Usage
============

LDP API
-------

The following are very basic examples of LDP interaction. For a more complete
reference, please consult the `Fedora API guide
<https://wiki.duraspace.org/display/FEDORA4x/RESTful+HTTP+API+-+Containers>`__.

**Note**: At the moment the LDP API only support the Turtle format for
serializing and deserializing RDF.

Create an empty LDP container (LDPC)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X POST http://localhost:8000/ldp


Create a resource with RDF payload
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X POST -H'Content-Type:text/turtle' --data-binary '<> <urn:ns:p1> <urn:ns:o1> .' http://localhost:8000/ldp


Create a resource at a specific location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X PUT http://localhost:8000/ldp/res1


Create a binary resource
~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X PUT -H'Content-Type:image/png' --data-binary '@/home/me/image.png' http://localhost:8000/ldp/bin1


Retrieve an RDF resource (LDP-RS)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl http://localhost:8000/ldp/res1

Retrieve a non-RDF source (LDP-NR)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl http://localhost:8000/ldp/bin1

Or::

    curl http://localhost:8000/ldp/bin1/fcr:content

Or::

    curl -H'Accept:image/png' http://localhost:8000/ldp/bin1

Retrieve RDF metadata of a LDP-NR
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl http://localhost:8000/ldp/bin1/fcr:metadata

Or::

    curl -H'Accept:text/turtle' http://localhost:8000/ldp/bin1


Soft-delete a resource
~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X DELETE http://localhost:8000/ldp/bin1


Restore ("resurrect") a resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X POST http://localhost:8000/ldp/bin1/fcr:tombstone


Permanently delete ("forget") a soft-deleted resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Note**: the following command cannot be issued after the previous one. It has
to be issued on a soft-deleted, non-resurrected resource.

::

    curl -X DELETE http://localhost:8000/ldp/bin1/fcr:tombstone

Immediately forget a resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X DELETE -H'Prefer:no-tombstone' http://localhost:8000/ldp/res1


Python API
----------

Set up the environment
~~~~~~~~~~~~~~~~~~~~~~

Before using the API, either do::

    >>> import lakesuperior.env_setup

Or, to specify an alternative configuration::

    >>> from lakesuperior import env
    >>> from lakesuperior.config_parser import parse_config
    >>> from lakesuperior.globals import AppGlobals
    >>> config = parse_config('/my/custom/config_dir')
    Reading configuration at /my/custom/config_dir
    >>> env.app_globals = AppGlobals(config)

Create and replace resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create an LDP-RS (RDF reseouce) providng a Graph object::

    >>> from rdflib import Graph, URIRef
    >>> uid = '/rsrc_from_graph'
    >>> gr = Graph().parse(data='<> a <http://ex.org/type#A> .',
    ...     format='text/turtle', publicID=nsc['fcres'][uid])
    >>> rsrc_api.create_or_replace(uid, init_gr=gr)

Issuing a ``create_or_replace()`` on an existing UID will replace the existing
property set with the provided one (PUT style).

Create an LDP-NR (non-RDF source)::

    >>> uid = '/test_ldpnr01'
    >>> data = b'Hello. This is some dummy content.'
    >>> rsrc_api.create_or_replace(
    ...     uid, stream=BytesIO(data), mimetype='text/plain')
    '_create_'

Create under a known parent, providing a slug (POST style)::

    >>> rsrc_api.create('/rsrc_from_stream', 'res1')


Retrieve Resources
~~~~~~~~~~~~~~~~~~

Retrieve a resource::

    >>> rsrc = rsrc_api.get('/rsrc_from_stream')
    >>> rsrc.uid
    '/rsrc_from_stream'
    >>> rsrc.uri
    rdflib.term.URIRef('info:fcres/rsrc_from_stream')
    >>> set(rsrc.metadata)
    {(rdflib.term.URIRef('info:fcres/rsrc_from_stream'),
      rdflib.term.URIRef('http://fedora.info/definitions/v4/repository#created'),
      rdflib.term.Literal('2018-04-06T03:30:49.460274+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime'))),
    [...]

Retrieve non-RDF content::

    >>> ldpnr = rsrc_api.get('/test_ldpnr01')
    >>> ldpnr.content.read()
    b'Hello. This is some dummy content.'

See the :doc:`API docs <api>` for more details on resource methods.

Update Resources
~~~~~~~~~~~~~~~~

Using a SPARQL update string::

    >>> uid = '/test_delta_patch_wc'
    >>> uri = nsc['fcres'][uid]
    >>> init_trp = {
    ...     (URIRef(uri), nsc['rdf'].type, nsc['foaf'].Person),
    ...     (URIRef(uri), nsc['foaf'].name, Literal('Joe Bob')),
    ...     (URIRef(uri), nsc['foaf'].name, Literal('Joe Average Bob')),
    ... }

    >>> update_str = '''
    ... DELETE {}
    ... INSERT { <> foaf:name "Joe Average 12oz Bob" . }
    ... WHERE {}
    ... '''

Using add/remove triple sets::

    >>> remove_trp = {
    ...     (URIRef(uri), nsc['foaf'].name, None),
    ... }
    >>> add_trp = {
    ...     (URIRef(uri), nsc['foaf'].name, Literal('Joan Knob')),
    ... }

    >>> gr = Graph()
    >>> gr += init_trp
    >>> rsrc_api.create_or_replace(uid, graph=gr)
    >>> rsrc_api.update_delta(uid, remove_trp, add_trp)

Note above that wildcards can be used, only in the remove triple set. Wherever
``None`` is used, all matches will be removed (in this example, all values of
``foaf:name``.

Generally speaking, the delta approach providing a set of remove triples and/or
a set of add triples is more convenient than SPARQL, which is a better fit for
complex query/update scenarios.
