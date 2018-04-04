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

**TODO**
