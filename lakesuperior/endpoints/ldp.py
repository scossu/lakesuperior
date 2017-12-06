import logging

from collections import defaultdict
from uuid import uuid4

from flask import Blueprint, current_app, g, request, send_file, url_for
from rdflib import Graph
from rdflib.namespace import RDF, XSD
from werkzeug.datastructures import FileStorage

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import *
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.model.ldp_nr import LdpNr
from lakesuperior.model.ldp_rs import Ldpc, LdpDc, LdpIc, LdpRs
from lakesuperior.toolbox import Toolbox


logger = logging.getLogger(__name__)


# Blueprint for LDP REST API. This is what is usually found under `/rest/` in
# standard fcrepo4. Here, it is under `/ldp` but initially `/rest` can be kept
# for backward compatibility.

ldp = Blueprint('ldp', __name__)

accept_patch = (
    'application/sparql-update',
)
accept_rdf = (
    'application/ld+json',
    'application/n-triples',
    'application/rdf+xml',
    #'application/x-turtle',
    #'application/xhtml+xml',
    #'application/xml',
    #'text/html',
    'text/n3',
    #'text/plain',
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
    'Accept-Post' : ','.join(accept_rdf),
    #'Allow' : ','.join(allow),
}

@ldp.url_defaults
def bp_url_defaults(endpoint, values):
    url_prefix = getattr(g, 'url_prefix', None)
    if url_prefix is not None:
        values.setdefault('url_prefix', url_prefix)

@ldp.url_value_preprocessor
def bp_url_value_preprocessor(endpoint, values):
    g.url_prefix = values.pop('url_prefix')


## REST SERVICES ##

@ldp.route('/<path:uuid>', methods=['GET'])
@ldp.route('/', defaults={'uuid': None}, methods=['GET'], strict_slashes=False)
def get_resource(uuid, force_rdf=False):
    '''
    Retrieve RDF or binary content.

    @param uuid (string) UUID of resource to retrieve.
    @param force_rdf (boolean) Whether to retrieve RDF even if the resource is
    a LDP-NR. This is not available in the API but is used e.g. by the
    `*/fcr:metadata` endpoint. The default is False.
    '''
    out_headers = std_headers
    repr_options = defaultdict(dict)
    if 'prefer' in request.headers:
        prefer = Toolbox().parse_rfc7240(request.headers['prefer'])
        logger.debug('Parsed Prefer header: {}'.format(prefer))
        if 'return' in prefer:
            repr_options = parse_repr_options(prefer['return'])

    try:
        rsrc = Ldpr.outbound_inst(uuid, repr_options)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        out_headers.update(rsrc.head())
        if isinstance(rsrc, LdpRs) \
                or request.headers['accept'] in accept_rdf \
                or force_rdf:
            return (rsrc.get(), out_headers)
        else:
            return send_file(rsrc.local_path, as_attachment=True,
                    attachment_filename=rsrc.filename)


@ldp.route('/<path:uuid>/fcr:metadata', methods=['GET'])
def get_metadata(uuid):
    '''
    Retrieve RDF metadata of a LDP-NR.
    '''
    return get_resource(uuid, force_rdf=True)


@ldp.route('/<path:parent>', methods=['POST'])
@ldp.route('/', defaults={'parent': None}, methods=['POST'],
        strict_slashes=False)
def post_resource(parent):
    '''
    Add a new resource in a new URI.
    '''
    out_headers = std_headers
    try:
        slug = request.headers['Slug']
    except KeyError:
        slug = None

    handling, disposition = set_post_put_params()
    stream, mimetype = bitstream_from_req()

    try:
        uuid = uuid_for_post(parent, slug)
        rsrc = Ldpr.inbound_inst(uuid, content_length=request.content_length,
                stream=stream, mimetype=mimetype, handling=handling,
                disposition=disposition)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)

    try:
        rsrc.post()
    except ServerManagedTermError as e:
        return str(e), 412

    out_headers.update({
        'Location' : rsrc.uri,
    })

    return rsrc.uri, 201, out_headers


@ldp.route('/<path:uuid>', methods=['PUT'])
def put_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    # Parse headers.
    logger.info('Request headers: {}'.format(request.headers))
    rsp_headers = std_headers

    handling, disposition = set_post_put_params()
    stream, mimetype = bitstream_from_req()

    try:
        rsrc = Ldpr.inbound_inst(uuid, content_length=request.content_length,
                stream=stream, mimetype=mimetype, handling=handling,
                disposition=disposition)
    except ServerManagedTermError as e:
        return str(e), 412
    except IncompatibleLdpTypeError as e:
        return str(e), 415

    try:
        ret = rsrc.put()
    except (InvalidResourceError, ResourceExistsError ) as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)

    res_code = 201 if ret == Ldpr.RES_CREATED else 204
    return '', res_code, rsp_headers


@ldp.route('/<path:uuid>', methods=['PATCH'])
def patch_resource(uuid):
    '''
    Update an existing resource with a SPARQL-UPDATE payload.
    '''
    headers = std_headers
    rsrc = Ldpc(uuid)

    try:
        rsrc.patch(request.get_data().decode('utf-8'))
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    except ServerManagedTermError as e:
        return str(e), 412

    return '', 204, headers


@ldp.route('/<path:uuid>', methods=['DELETE'])
def delete_resource(uuid):
    '''
    Delete a resource.
    '''
    headers = std_headers

    # If referential integrity is enforced, grab all inbound relationships
    # to break them.
    repr_opts = {'incl_inbound' : True} \
            if current_app.config['store']['ldp_rs']['referential_integrity'] \
            else {}
    if 'prefer' in request.headers:
        prefer = Toolbox().parse_rfc7240(request.headers['prefer'])
        leave_tstone = 'no-tombstone' not in prefer
    else:
        leave_tstone = True

    try:
        Ldpr.outbound_inst(uuid, repr_opts).delete(leave_tstone=leave_tstone)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)

    return '', 204, headers


@ldp.route('/<path:uuid>/fcr:tombstone', methods=['GET', 'POST', 'PUT',
        'PATCH', 'DELETE'])
def tombstone(uuid):
    '''
    Handle all tombstone operations.

    The only allowed method is DELETE; any other verb will return a 405.
    '''
    logger.debug('Deleting tombstone for {}.'.format(uuid))
    rsrc = Ldpr(uuid)
    try:
        imr = rsrc.imr
    except TombstoneError as e:
        if request.method == 'DELETE':
            if e.uuid == uuid:
                rsrc.delete_tombstone()
                return '', 204
            else:
                return _tombstone_response(e, uuid)
        else:
            return 'Method Not Allowed.', 405
    except ResourceNotExistsError as e:
        return str(e), 404
    else:
        return '', 404


def uuid_for_post(parent_uuid=None, slug=None):
    '''
    Validate conditions to perform a POST and return an LDP resource
    UUID for using with the `post` method.

    This may raise an exception resulting in a 404 if the parent is not
    found or a 409 if the parent is not a valid container.
    '''
    # Shortcut!
    if not slug and not parent_uuid:
        return str(uuid4())

    parent = Ldpr.outbound_inst(parent_uuid, repr_opts={'incl_children' : False})

    # Set prefix.
    if parent_uuid:
        parent_types = { t.identifier for t in \
                parent.imr.objects(RDF.type) }
        logger.debug('Parent types: {}'.format(
                parent_types))
        if nsc['ldp'].Container not in parent_types:
            raise InvalidResourceError('Parent {} is not a container.'
                   .format(parent_uuid))

        pfx = parent_uuid + '/'
    else:
        pfx = ''

    # Create candidate UUID and validate.
    if slug:
        cnd_uuid = pfx + slug
        if current_app.rdfly.ask_rsrc_exists(nsc['fcres'][cnd_uuid]):
            uuid = pfx + str(uuid4())
        else:
            uuid = cnd_uuid
    else:
        uuid = pfx + str(uuid4())

    return uuid


def bitstream_from_req():
    '''
    Find how a binary file and its MIMEtype were uploaded in the request.
    '''
    logger.debug('Content type: {}'.format(request.mimetype))
    logger.debug('files: {}'.format(request.files))
    logger.debug('stream: {}'.format(request.stream))

    if request.mimetype == 'multipart/form-data':
        # This seems the "right" way to upload a binary file, with a
        # multipart/form-data MIME type and the file in the `file`
        # field. This however is not supported by FCREPO4.
        stream = request.files.get('file').stream
        mimetype = request.files.get('file').content_type
        # @TODO This will turn out useful to provide metadata
        # with the binary.
        #metadata = request.files.get('metadata').stream
        #provided_imr = [parse RDF here...]
    else:
        # This is a less clean way, with the file in the form body and
        # the request as application/x-www-form-urlencoded.
        # This is how FCREPO4 accepts binary uploads.
        stream = request.stream
        mimetype = request.mimetype

    return stream, mimetype


def _get_bitstream(rsrc):
    out_headers = std_headers

    # @TODO This may change in favor of more low-level handling if the file
    # system is not local.
    return send_file(rsrc.local_path, as_attachment=True,
            attachment_filename=rsrc.filename)


def _tombstone_response(e, uuid):
    headers = {
        'Link' : '<{}/fcr:tombstone>; rel="hasTombstone"'.format(request.url),
    } if e.uuid == uuid else {}
    return str(e), 410, headers



def set_post_put_params():
    '''
    Sets handling and content disposition for POST and PUT by parsing headers.
    '''
    handling = None
    if 'prefer' in request.headers:
        prefer = Toolbox().parse_rfc7240(request.headers['prefer'])
        logger.debug('Parsed Prefer header: {}'.format(prefer))
        if 'handling' in prefer:
            handling = prefer['handling']['value']

    try:
        disposition = Toolbox().parse_rfc7240(
                request.headers['content-disposition'])
    except KeyError:
        disposition = None

    return handling, disposition


def parse_repr_options(retr_opts):
    '''
    Set options to retrieve IMR.

    Ideally, IMR retrieval is done once per request, so all the options
    are set once in the `imr()` property.

    @param retr_opts (dict): Options parsed from `Prefer` header.
    '''
    logger.debug('Parsing retrieval options: {}'.format(retr_opts))
    imr_options = {}

    if retr_opts.setdefault('value') == 'minimal':
        imr_options = {
            'embed_children' : False,
            'incl_children' : False,
            'incl_inbound' : False,
            'incl_srv_mgd' : False,
        }
    else:
        # Default.
        imr_options = {
            'embed_children' : False,
            'incl_children' : True,
            'incl_inbound' : False,
            'incl_srv_mgd' : True,
        }

        # Override defaults.
        if 'parameters' in retr_opts:
            include = retr_opts['parameters']['include'].split(' ') \
                    if 'include' in retr_opts['parameters'] else []
            omit = retr_opts['parameters']['omit'].split(' ') \
                    if 'omit' in retr_opts['parameters'] else []

            logger.debug('Include: {}'.format(include))
            logger.debug('Omit: {}'.format(omit))

            if str(Ldpr.EMBED_CHILD_RES_URI) in include:
                    imr_options['embed_children'] = True
            if str(Ldpr.RETURN_CHILD_RES_URI) in omit:
                    imr_options['incl_children'] = False
            if str(Ldpr.RETURN_INBOUND_REF_URI) in include:
                    imr_options['incl_inbound'] = True
            if str(Ldpr.RETURN_SRV_MGD_RES_URI) in omit:
                    imr_options['incl_srv_mgd'] = False

    logger.debug('Retrieval options: {}'.format(imr_options))

    return imr_options


