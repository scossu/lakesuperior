from flask import Blueprint, request

from lakesuperior.ldp.ldpr import Ldpr, Ldpc, LdpNr, \
        InvalidResourceError, ResourceNotExistsError


# Blueprint for LDP REST API. This is what is usually found under `/rest/` in
# standard fcrepo4. Here, it is under `/ldp` but initially `/rest` can be kept
# for backward compatibility.

ldp = Blueprint('ldp', __name__)

accept_patch = (
    'application/sparql-update',
)
accept_post = (
    'application/ld+json',
    'application/n-triples',
    'application/rdf+xml',
    'application/x-turtle',
    'application/xhtml+xml',
    'application/xml',
    'text/html',
    'text/n3',
    'text/plain',
    'text/rdf+n3',
    'text/turtle',
)
#allow = (
#    'COPY',
#    'DELETE',
#    'GET',
#    'HEAD',
#    'MOVE',
#    'OPTIONS',
#    'PATCH',
#    'POST',
#    'PUT',
#)

std_headers = {
    'Accept-Patch' : ','.join(accept_patch),
    'Accept-Post' : ','.join(accept_post),
    #'Allow' : ','.join(allow),
}


## REST SERVICES ##

@ldp.route('/<path:uuid>', methods=['GET'])
@ldp.route('/', defaults={'uuid': None}, methods=['GET'],
        strict_slashes=False)
def get_resource(uuid):
    '''
    Retrieve RDF or binary content.
    '''
    headers = std_headers
    # @TODO Add conditions for LDP-NR
    rsrc = Ldpc(uuid)
    try:
        out = rsrc.get()
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404
    else:
        headers = rsrc.head()
        return (out.graph.serialize(format='turtle'), headers)


@ldp.route('/<path:parent>', methods=['POST'])
@ldp.route('/', defaults={'parent': None}, methods=['POST'],
        strict_slashes=False)
def post_resource(parent):
    '''
    Add a new resource in a new URI.
    '''
    headers = std_headers
    try:
        slug = request.headers['Slug']
    except KeyError:
        slug = None

    try:
       rsrc = Ldpc.inst_for_post(parent, slug)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409

    rsrc.post(request.get_data().decode('utf-8'))

    headers.update({
        'Location' : rsrc.uri,
    })

    return rsrc.uri, headers, 201


@ldp.route('/<path:uuid>', methods=['PUT'])
def put_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    headers = std_headers
    rsrc = Ldpc(uuid)

    rsrc.put(request.get_data().decode('utf-8'))
    return '', 204, headers


@ldp.route('/<path:uuid>', methods=['PATCH'])
def patch_resource(uuid):
    '''
    Update an existing resource with a SPARQL-UPDATE payload.
    '''
    headers = std_headers
    rsrc = Ldpc(uuid)

    try:
        rsrc.patch(request.get_data().decode('utf-8'))
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404

    return '', 204, headers


@ldp.route('/<path:uuid>', methods=['DELETE'])
def delete_resource(uuid):
    '''
    Delete a resource.
    '''
    headers = std_headers
    rsrc = Ldpc(uuid)

    try:
        rsrc.delete()
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404

    return '', 204, headers

