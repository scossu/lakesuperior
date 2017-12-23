import logging

from collections import defaultdict
from pprint import pformat
from uuid import uuid4

import arrow

from flask import (
        Blueprint, current_app, g, make_response, render_template,
        request, send_file)
from rdflib.namespace import RDF, XSD
from rdflib.term import Literal

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.exceptions import *
from lakesuperior.model.ldp_factory import LdpFactory
from lakesuperior.model.ldp_nr import LdpNr
from lakesuperior.model.ldp_rs import LdpRs
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.toolbox import Toolbox


logger = logging.getLogger(__name__)


# Blueprint for LDP REST API. This is what is usually found under `/rest/` in
# standard fcrepo4. Here, it is under `/ldp` but initially `/rest` can be kept
# for backward compatibility.

ldp = Blueprint(
        'ldp', __name__, template_folder='templates',
        static_url_path='/static', static_folder='../../static')

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

std_headers = {
    'Accept-Patch' : ','.join(accept_patch),
    'Accept-Post' : ','.join(accept_rdf),
    #'Allow' : ','.join(allow),
}

'''Predicates excluded by view.'''
vw_blacklist = {
    nsc['fcrepo'].contains,
}

@ldp.url_defaults
def bp_url_defaults(endpoint, values):
    url_prefix = getattr(g, 'url_prefix', None)
    if url_prefix is not None:
        values.setdefault('url_prefix', url_prefix)

@ldp.url_value_preprocessor
def bp_url_value_preprocessor(endpoint, values):
    g.url_prefix = values.pop('url_prefix')
    g.webroot = request.host_url + g.url_prefix


@ldp.before_request
def log_request_start():
    logger.info('\n\n** Start {} {} **'.format(request.method, request.url))


@ldp.before_request
def instantiate_toolbox():
    g.tbox = Toolbox()


@ldp.before_request
def request_timestamp():
    g.timestamp = arrow.utcnow()
    g.timestamp_term = Literal(g.timestamp, datatype=XSD.dateTime)


@ldp.after_request
def log_request_end(rsp):
    logger.info('** End {} {} **\n\n'.format(request.method, request.url))

    return rsp


## REST SERVICES ##

@ldp.route('/<path:uuid>', methods=['GET'], strict_slashes=False)
@ldp.route('/', defaults={'uuid': None}, methods=['GET'], strict_slashes=False)
@ldp.route('/<path:uuid>/fcr:metadata', defaults={'force_rdf' : True},
        methods=['GET'])
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
        prefer = g.tbox.parse_rfc7240(request.headers['prefer'])
        logger.debug('Parsed Prefer header: {}'.format(pformat(prefer)))
        if 'return' in prefer:
            repr_options = parse_repr_options(prefer['return'])

    try:
        rsrc = LdpFactory.from_stored(uuid, repr_options)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        out_headers.update(rsrc.head())
        if isinstance(rsrc, LdpRs) \
                or is_accept_hdr_rdf_parsable() \
                or force_rdf:
            resp = rsrc.get()
            if request.accept_mimetypes.best == 'text/html':
                rsrc = resp.resource(request.path)
                return render_template(
                        'resource.html', rsrc=rsrc, nsm=nsm,
                        blacklist = vw_blacklist)
            else:
                for p in vw_blacklist:
                    resp.remove((None, p, None))
                return (resp.serialize(format='turtle'), out_headers)
        else:
            logger.info('Streaming out binary content.')
            rsp = make_response(send_file(rsrc.local_path, as_attachment=True,
                    attachment_filename=rsrc.filename))
            rsp.headers['Link'] = '<{}/fcr:metadata>; rel="describedby"'\
                    .format(rsrc.uri)

            return rsp


@ldp.route('/<path:parent>', methods=['POST'], strict_slashes=False)
@ldp.route('/', defaults={'parent': None}, methods=['POST'],
        strict_slashes=False)
def post_resource(parent):
    '''
    Add a new resource in a new URI.
    '''
    out_headers = std_headers
    try:
        slug = request.headers['Slug']
        logger.info('Slug: {}'.format(slug))
    except KeyError:
        slug = None

    handling, disposition = set_post_put_params()
    stream, mimetype = bitstream_from_req()

    try:
        uuid = uuid_for_post(parent, slug)
        logger.debug('Generated UUID for POST: {}'.format(uuid))
        rsrc = LdpFactory.from_provided(uuid, content_length=request.content_length,
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

    hdr = {
        'Location' : rsrc.uri,
    }

    if isinstance(rsrc, LdpNr):
        hdr['Link'] = '<{0}/fcr:metadata>; rel="describedby"; anchor="<{0}>"'\
                .format(rsrc.uri)

    out_headers.update(hdr)

    return rsrc.uri, 201, out_headers


@ldp.route('/<path:uuid>/fcr:versions', methods=['GET'])
def get_version_info(uuid):
    '''
    Get version info (`fcr:versions`).
    '''
    try:
        rsp = Ldpr(uuid).get_version_info()
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        return rsp.serialize(format='turtle'), 200


@ldp.route('/<path:uuid>/fcr:versions/<ver_uid>', methods=['GET'])
def get_version(uuid, ver_uid):
    '''
    Get an individual resource version.

    @param uuid (string) Resource UUID.
    @param ver_uid (string) Version UID.
    '''
    try:
        rsp = Ldpr(uuid).get_version(ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        return rsp.serialize(format='turtle'), 200


@ldp.route('/<path:uuid>/fcr:versions', methods=['POST'])
def post_version(uuid):
    '''
    Create a new resource version.
    '''
    ver_uid = request.headers.get('slug', None)
    try:
        ver_uri = LdpFactory.from_stored(uuid).create_version(ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        return '', 201, {'Location': ver_uri}


@ldp.route('/<path:uuid>/fcr:versions/<ver_uid>', methods=['PATCH'])
def patch_version(uuid, ver_uid):
    '''
    Revert to a previous version.

    NOTE: This creates a new version snapshot.

    @param uuid (string) Resource UUID.
    @param ver_uid (string) Version UID.
    '''
    try:
        LdpFactory.from_stored(uuid).revert_to_version(ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        return '', 204


@ldp.route('/<path:uuid>', methods=['PUT'], strict_slashes=False)
@ldp.route('/<path:uuid>/fcr:metadata', defaults={'force_rdf' : True},
        methods=['PUT'])
def put_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    # Parse headers.
    logger.info('Request headers: {}'.format(request.headers))

    rsp_headers = {'Content-Type' : 'text/plain; charset=utf-8'}

    handling, disposition = set_post_put_params()
    stream, mimetype = bitstream_from_req()

    try:
        rsrc = LdpFactory.from_provided(uuid, content_length=request.content_length,
                stream=stream, mimetype=mimetype, handling=handling,
                disposition=disposition)
        if not request.content_length and rsrc.is_stored:
            raise InvalidResourceError(
                rsrc.uuid, 'Resource already exists and no data was provided.')
    except InvalidResourceError as e:
        return str(e), 409
    except (ServerManagedTermError, SingleSubjectError) as e:
        return str(e), 412
    except IncompatibleLdpTypeError as e:
        return str(e), 415

    try:
        ret = rsrc.put()
    except (InvalidResourceError, ResourceExistsError) as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)

    rsp_headers.update(rsrc.head())
    if ret == Ldpr.RES_CREATED:
        rsp_code = 201
        rsp_headers['Location'] = rsp_body = rsrc.uri
        if isinstance(rsrc, LdpNr):
            rsp_headers['Link'] = '<{0}/fcr:metadata>; rel="describedby"'\
                    .format(rsrc.uri)
    else:
        rsp_code = 204
        rsp_body = ''
    return rsp_body, rsp_code, rsp_headers


@ldp.route('/<path:uuid>', methods=['PATCH'], strict_slashes=False)
def patch_resource(uuid):
    '''
    Update an existing resource with a SPARQL-UPDATE payload.
    '''
    rsp_headers = {'Content-Type' : 'text/plain; charset=utf-8'}
    rsrc = LdpRs(uuid)
    if request.mimetype != 'application/sparql-update':
        return 'Provided content type is not a valid parsable format: {}'\
                .format(request.mimetype), 415

    try:
        rsrc.patch(request.get_data().decode('utf-8'))
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    except (ServerManagedTermError, SingleSubjectError) as e:
        return str(e), 412
    else:
        rsp_headers.update(rsrc.head())
        return '', 204, rsp_headers


@ldp.route('/<path:uuid>/fcr:metadata', methods=['PATCH'])
def patch_resource_metadata(uuid):
    return patch_resource(uuid)


@ldp.route('/<path:uuid>', methods=['DELETE'])
def delete_resource(uuid):
    '''
    Delete a resource and optionally leave a tombstone.

    This behaves differently from FCREPO. A tombstone indicated that the
    resource is no longer available at its current location, but its historic
    snapshots still are. Also, deleting a resource with a tombstone creates
    one more version snapshot of the resource prior to being deleted.

    In order to completely wipe out all traces of a resource, the tombstone
    must be deleted as well, or the `Prefer:no-tombstone` header can be used.
    The latter will purge the resource immediately.
    '''
    headers = std_headers

    # If referential integrity is enforced, grab all inbound relationships
    # to break them.
    repr_opts = {'incl_inbound' : True} \
            if current_app.config['store']['ldp_rs']['referential_integrity'] \
            else {}
    if 'prefer' in request.headers:
        prefer = g.tbox.parse_rfc7240(request.headers['prefer'])
        leave_tstone = 'no-tombstone' not in prefer
    else:
        leave_tstone = True

    try:
        LdpFactory.from_stored(uuid, repr_opts).delete(leave_tstone=leave_tstone)
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

    The only allowed methods are POST and DELETE; any other verb will return a
    405.
    '''
    logger.debug('Deleting tombstone for {}.'.format(uuid))
    rsrc = Ldpr(uuid)
    try:
        imr = rsrc.imr
    except TombstoneError as e:
        if request.method == 'DELETE':
            if e.uuid == uuid:
                rsrc.purge()
                return '', 204
            else:
                return _tombstone_response(e, uuid)
        elif request.method == 'POST':
            if e.uuid == uuid:
                rsrc_uri = rsrc.resurrect()
                headers = {'Location' : rsrc_uri}
                return rsrc_uri, 201, headers
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
    def split_if_legacy(uuid):
        if current_app.config['store']['ldp_rs']['legacy_ptree_split']:
            uuid = g.tbox.split_uuid(uuid)
        return uuid

    # Shortcut!
    if not slug and not parent_uuid:
        uuid = split_if_legacy(str(uuid4()))

        return uuid

    parent = LdpFactory.from_stored(parent_uuid, repr_opts={'incl_children' : False})

    if nsc['fcrepo'].Pairtree in parent.types:
        raise InvalidResourceError(parent.uuid,
                'Resources cannot be created under a pairtree.')

    # Set prefix.
    if parent_uuid:
        parent_types = { t.identifier for t in \
                parent.imr.objects(RDF.type) }
        logger.debug('Parent types: {}'.format(pformat(parent_types)))
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
            uuid = pfx + split_if_legacy(str(uuid4()))
        else:
            uuid = cnd_uuid
    else:
        uuid = pfx + split_if_legacy(str(uuid4()))

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
        prefer = g.tbox.parse_rfc7240(request.headers['prefer'])
        logger.debug('Parsed Prefer header: {}'.format(prefer))
        if 'handling' in prefer:
            handling = prefer['handling']['value']

    try:
        disposition = g.tbox.parse_rfc7240(
                request.headers['content-disposition'])
    except KeyError:
        disposition = None

    return handling, disposition


def is_accept_hdr_rdf_parsable():
    '''
    Check if any of the 'Accept' header values provided is a RDF parsable
    format.
    '''
    for mimetype in request.accept_mimetypes.values():
        if LdpFactory.is_rdf_parsable(mimetype):
            return True
    return False


def parse_repr_options(retr_opts):
    '''
    Set options to retrieve IMR.

    Ideally, IMR retrieval is done once per request, so all the options
    are set once in the `imr()` property.

    @param retr_opts (dict): Options parsed from `Prefer` header.
    '''
    logger.debug('Parsing retrieval options: {}'.format(retr_opts))
    imr_options = {}

    if retr_opts.get('value') == 'minimal':
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

    logger.debug('Retrieval options: {}'.format(pformat(imr_options)))

    return imr_options


