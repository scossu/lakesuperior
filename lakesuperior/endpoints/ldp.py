import logging
import pdb

from collections import defaultdict
from io import BytesIO
from pprint import pformat
from uuid import uuid4

import arrow

from flask import (
        Blueprint, Response, g, make_response, render_template,
        request, send_file)
from rdflib import Graph, plugin, parser#, serializer

from lakesuperior.api import resource as rsrc_api
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.exceptions import (ResourceNotExistsError, TombstoneError,
        ServerManagedTermError, InvalidResourceError, SingleSubjectError,
        ResourceExistsError, IncompatibleLdpTypeError)
from lakesuperior.globals import RES_CREATED
from lakesuperior.model.ldp_factory import LdpFactory
from lakesuperior.model.ldp_nr import LdpNr
from lakesuperior.model.ldp_rs import LdpRs
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager
from lakesuperior.toolbox import Toolbox


DEFAULT_RDF_MIMETYPE = 'text/turtle'
"""
Fallback serialization format used when no acceptable formats are specified.
"""

logger = logging.getLogger(__name__)
rdf_parsable_mimetypes = {
    mt.name for mt in plugin.plugins()
    if mt.kind is parser.Parser and '/' in mt.name
}
"""MIMEtypes that can be parsed into RDF."""

rdf_serializable_mimetypes = {
    #mt.name for mt in plugin.plugins()
    #if mt.kind is serializer.Serializer and '/' in mt.name
    'application/ld+json',
    'application/n-triples',
    'application/rdf+xml',
    'text/turtle',
    'text/n3',
}
"""
MIMEtypes that RDF can be serialized into.

These are not automatically derived from RDFLib because only triple
(not quad) serializations are applicable.
"""

accept_patch = (
    'application/sparql-update',
)

std_headers = {
    'Accept-Patch' : ','.join(accept_patch),
    'Accept-Post' : ','.join(rdf_parsable_mimetypes),
}

"""Predicates excluded by view."""
vw_blacklist = {
}


ldp = Blueprint(
        'ldp', __name__, template_folder='templates',
        static_url_path='/static', static_folder='templates/static')
"""
Blueprint for LDP REST API. This is what is usually found under ``/rest/`` in
standard fcrepo4. Here, it is under ``/ldp`` but initially ``/rest`` will be
kept for backward compatibility.
"""

## ROUTE PRE- & POST-PROCESSING ##

@ldp.url_defaults
def bp_url_defaults(endpoint, values):
    url_prefix = getattr(g, 'url_prefix', None)
    if url_prefix is not None:
        values.setdefault('url_prefix', url_prefix)


@ldp.url_value_preprocessor
def bp_url_value_preprocessor(endpoint, values):
    g.url_prefix = values.pop('url_prefix')
    g.webroot = request.host_url + g.url_prefix
    # Normalize leading slashes for UID.
    if 'uid' in values:
        values['uid'] = '/' + values['uid'].lstrip('/')
    if 'parent_uid' in values:
        values['parent_uid'] = '/' + values['parent_uid'].lstrip('/')


@ldp.before_request
def log_request_start():
    logger.info('** Start {} {} **'.format(request.method, request.url))


@ldp.before_request
def instantiate_req_vars():
    g.tbox = Toolbox()


@ldp.after_request
def log_request_end(rsp):
    logger.info('** End {} {} **'.format(request.method, request.url))

    return rsp


## REST SERVICES ##

@ldp.route('/<path:uid>', methods=['GET'], strict_slashes=False)
@ldp.route('/', defaults={'uid': '/'}, methods=['GET'], strict_slashes=False)
@ldp.route('/<path:uid>/fcr:metadata', defaults={'out_fmt' : 'rdf'},
        methods=['GET'])
@ldp.route('/<path:uid>/fcr:content', defaults={'out_fmt' : 'non_rdf'},
        methods=['GET'])
def get_resource(uid, out_fmt=None):
    r"""
    https://www.w3.org/TR/ldp/#ldpr-HTTP_GET

    Retrieve RDF or binary content.

    :param str uid: UID of resource to retrieve. The repository root has
        an empty string for UID.
    :param str out_fmt: Force output to RDF or non-RDF if the resource is
        a LDP-NR. This is not available in the API but is used e.g. by the
        ``\*/fcr:metadata`` and ``\*/fcr:content`` endpoints. The default is
        False.
    """
    logger.info('UID: {}'.format(uid))
    out_headers = std_headers
    repr_options = defaultdict(dict)
    if 'prefer' in request.headers:
        prefer = g.tbox.parse_rfc7240(request.headers['prefer'])
        logger.debug('Parsed Prefer header: {}'.format(pformat(prefer)))
        if 'return' in prefer:
            repr_options = parse_repr_options(prefer['return'])

    try:
        rsrc = rsrc_api.get(uid, repr_options)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        if out_fmt is None:
            rdf_mimetype = _best_rdf_mimetype()
            out_fmt = (
                    'rdf'
                    if isinstance(rsrc, LdpRs) or rdf_mimetype is not None
                    else 'non_rdf')
        out_headers.update(_headers_from_metadata(rsrc))
        uri = g.tbox.uid_to_uri(uid)
        if out_fmt == 'rdf':
            if locals().get('rdf_mimetype', None) is None:
                rdf_mimetype = DEFAULT_RDF_MIMETYPE
            ggr = g.tbox.globalize_graph(rsrc.out_graph)
            ggr.namespace_manager = nsm
            return _negotiate_content(
                    ggr, rdf_mimetype, out_headers, uid=uid, uri=uri)
        else:
            if not getattr(rsrc, 'local_path', False):
                return ('{} has no binary content.'.format(rsrc.uid), 404)

            logger.debug('Streaming out binary content.')
            rsp = make_response(send_file(
                    rsrc.local_path, as_attachment=True,
                    attachment_filename=rsrc.filename,
                    mimetype=rsrc.mimetype))
            logger.debug('Out headers: {}'.format(out_headers))
            rsp.headers.add('Link',
                    '<{}/fcr:metadata>; rel="describedby"'.format(uri))
            for link in out_headers['Link']:
                rsp.headers.add('Link', link)
            return rsp


@ldp.route('/<path:uid>/fcr:versions', methods=['GET'])
def get_version_info(uid):
    """
    Get version info (`fcr:versions`).

    :param str uid: UID of resource to retrieve versions for.
    """
    rdf_mimetype = _best_rdf_mimetype() or DEFAULT_RDF_MIMETYPE
    try:
        gr = rsrc_api.get_version_info(uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return _negotiate_content(g.tbox.globalize_graph(gr), rdf_mimetype)


@ldp.route('/<path:uid>/fcr:versions/<ver_uid>', methods=['GET'])
def get_version(uid, ver_uid):
    """
    Get an individual resource version.

    :param str uid: Resource UID.
    :param str ver_uid: Version UID.
    """
    rdf_mimetype = _best_rdf_mimetype() or DEFAULT_RDF_MIMETYPE
    try:
        gr = rsrc_api.get_version(uid, ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return _negotiate_content(g.tbox.globalize_graph(gr), rdf_mimetype)


@ldp.route('/<path:parent_uid>', methods=['POST'], strict_slashes=False)
@ldp.route('/', defaults={'parent_uid': '/'}, methods=['POST'],
        strict_slashes=False)
def post_resource(parent_uid):
    """
    https://www.w3.org/TR/ldp/#ldpr-HTTP_POST

    Add a new resource in a new URI.
    """
    out_headers = std_headers
    try:
        slug = request.headers['Slug']
        logger.debug('Slug: {}'.format(slug))
    except KeyError:
        slug = None

    handling, disposition = set_post_put_params()
    stream, mimetype = _bistream_from_req()

    if mimetype in rdf_parsable_mimetypes:
        # If the content is RDF, localize in-repo URIs.
        global_rdf = stream.read()
        rdf_data = g.tbox.localize_payload(global_rdf)
        rdf_fmt = mimetype
        stream = mimetype = None
    else:
        rdf_data = rdf_fmt = None

    try:
        uid = rsrc_api.create(
            parent_uid, slug, stream=stream, mimetype=mimetype,
            rdf_data=rdf_data, rdf_fmt=rdf_fmt, handling=handling,
            disposition=disposition)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    except ServerManagedTermError as e:
        return str(e), 412

    uri = g.tbox.uid_to_uri(uid)
    hdr = {'Location' : uri}

    if mimetype and rdf_fmt is None:
        hdr['Link'] = '<{0}/fcr:metadata>; rel="describedby"; anchor="{0}"'\
                .format(uri)

    out_headers.update(hdr)

    return uri, 201, out_headers


@ldp.route('/<path:uid>', methods=['PUT'], strict_slashes=False)
@ldp.route('/<path:uid>/fcr:metadata', defaults={'force_rdf' : True},
        methods=['PUT'])
def put_resource(uid):
    """
    https://www.w3.org/TR/ldp/#ldpr-HTTP_PUT

    Add or replace a new resource at a specified URI.
    """
    # Parse headers.
    logger.debug('Request headers: {}'.format(request.headers))

    rsp_headers = {'Content-Type' : 'text/plain; charset=utf-8'}

    handling, disposition = set_post_put_params()
    stream, mimetype = _bistream_from_req()

    if mimetype in rdf_parsable_mimetypes:
        # If the content is RDF, localize in-repo URIs.
        global_rdf = stream.read()
        rdf_data = g.tbox.localize_payload(global_rdf)
        rdf_fmt = mimetype
        stream = mimetype = None
    else:
        rdf_data = rdf_fmt = None

    try:
        evt = rsrc_api.create_or_replace(
            uid, stream=stream, mimetype=mimetype,
            rdf_data=rdf_data, rdf_fmt=rdf_fmt, handling=handling,
            disposition=disposition)
    except (InvalidResourceError, ResourceExistsError) as e:
        return str(e), 409
    except (ServerManagedTermError, SingleSubjectError) as e:
        return str(e), 412
    except IncompatibleLdpTypeError as e:
        return str(e), 415
    except TombstoneError as e:
        return _tombstone_response(e, uid)

    uri = g.tbox.uid_to_uri(uid)
    if evt == RES_CREATED:
        rsp_code = 201
        rsp_headers['Location'] = rsp_body = uri
        if mimetype and not rdf_data:
            rsp_headers['Link'] = (
                    '<{0}/fcr:metadata>; rel="describedby"'.format(uri))
    else:
        rsp_code = 204
        rsp_body = ''
    return rsp_body, rsp_code, rsp_headers


@ldp.route('/<path:uid>', methods=['PATCH'], strict_slashes=False)
@ldp.route('/', defaults={'uid': '/'}, methods=['PATCH'],
        strict_slashes=False)
def patch_resource(uid, is_metadata=False):
    """
    https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH

    Update an existing resource with a SPARQL-UPDATE payload.
    """
    rsp_headers = {'Content-Type' : 'text/plain; charset=utf-8'}
    if request.mimetype != 'application/sparql-update':
        return 'Provided content type is not a valid parsable format: {}'\
                .format(request.mimetype), 415

    update_str = request.get_data().decode('utf-8')
    local_update_str = g.tbox.localize_ext_str(update_str, nsc['fcres'][uid])
    try:
        rsrc = rsrc_api.update(uid, local_update_str, is_metadata)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    except (ServerManagedTermError, SingleSubjectError) as e:
        return str(e), 412
    except InvalidResourceError as e:
        return str(e), 415
    else:
        rsp_headers.update(_headers_from_metadata(rsrc))
        return '', 204, rsp_headers


@ldp.route('/<path:uid>/fcr:metadata', methods=['PATCH'])
def patch_resource_metadata(uid):
    return patch_resource(uid, True)


@ldp.route('/<path:uid>', methods=['DELETE'])
def delete_resource(uid):
    """
    Delete a resource and optionally leave a tombstone.

    This behaves differently from FCREPO. A tombstone indicated that the
    resource is no longer available at its current location, but its historic
    snapshots still are. Also, deleting a resource with a tombstone creates
    one more version snapshot of the resource prior to being deleted.

    In order to completely wipe out all traces of a resource, the tombstone
    must be deleted as well, or the ``Prefer:no-tombstone`` header can be used.
    The latter will forget (completely delete) the resource immediately.
    """
    headers = std_headers

    if 'prefer' in request.headers:
        prefer = g.tbox.parse_rfc7240(request.headers['prefer'])
        leave_tstone = 'no-tombstone' not in prefer
    else:
        leave_tstone = True

    try:
        rsrc_api.delete(uid, leave_tstone)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uid)

    return '', 204, headers


@ldp.route('/<path:uid>/fcr:tombstone', methods=['GET', 'POST', 'PUT',
        'PATCH', 'DELETE'])
def tombstone(uid):
    """
    Handle all tombstone operations.

    The only allowed methods are POST and DELETE; any other verb will return a
    405.
    """
    try:
        rsrc = rsrc_api.get(uid)
    except TombstoneError as e:
        if request.method == 'DELETE':
            if e.uid == uid:
                rsrc_api.delete(uid, False)
                return '', 204
            else:
                return _tombstone_response(e, uid)
        elif request.method == 'POST':
            if e.uid == uid:
                rsrc_uri = rsrc_api.resurrect(uid)
                headers = {'Location' : rsrc_uri}
                return rsrc_uri, 201, headers
            else:
                return _tombstone_response(e, uid)
        else:
            return 'Method Not Allowed.', 405
    except ResourceNotExistsError as e:
        return str(e), 404
    else:
        return '', 404


@ldp.route('/<path:uid>/fcr:versions', methods=['POST', 'PUT'])
def post_version(uid):
    """
    Create a new resource version.
    """
    if request.method == 'PUT':
        return 'Method not allowed.', 405
    ver_uid = request.headers.get('slug', None)

    try:
        ver_uid = rsrc_api.create_version(uid, ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return '', 201, {'Location': g.tbox.uid_to_uri(ver_uid)}


@ldp.route('/<path:uid>/fcr:versions/<ver_uid>', methods=['PATCH'])
def patch_version(uid, ver_uid):
    """
    Revert to a previous version.

    NOTE: This creates a new version snapshot.

    :param str uid: Resource UID.
    :param str ver_uid: Version UID.
    """
    try:
        rsrc_api.revert_to_version(uid, rsrc_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return '', 204


## PRIVATE METHODS ##

def _best_rdf_mimetype():
    """
    Check if any of the 'Accept' header values provided is a RDF parsable
    format.
    """
    for accept in request.accept_mimetypes:
        mimetype = accept[0]
        if mimetype in rdf_parsable_mimetypes:
            return mimetype
    return None


def _negotiate_content(gr, rdf_mimetype, headers=None, **vw_kwargs):
    """
    Return HTML or serialized RDF depending on accept headers.
    """
    if request.accept_mimetypes.best == 'text/html':
        return render_template(
                'resource.html', gr=gr, nsc=nsc, nsm=nsm,
                blacklist=vw_blacklist, arrow=arrow, **vw_kwargs)
    else:
        for p in vw_blacklist:
            gr.remove((None, p, None))
        return Response(
                gr.serialize(format=rdf_mimetype), 200, headers,
                mimetype=rdf_mimetype)


def _bistream_from_req():
    """
    Find how a binary file and its MIMEtype were uploaded in the request.
    """
    #logger.debug('Content type: {}'.format(request.mimetype))
    #logger.debug('files: {}'.format(request.files))
    #logger.debug('stream: {}'.format(request.stream))

    if request.mimetype == 'multipart/form-data':
        # This seems the "right" way to upload a binary file, with a
        # multipart/form-data MIME type and the file in the `file`
        # field. This however is not supported by FCREPO4.
        stream = request.files.get('file').stream
        mimetype = request.files.get('file').content_type
        # @TODO This will turn out useful to provide metadata
        # with the binary.
        #metadata = request.files.get('metadata').stream
    else:
        # This is a less clean way, with the file in the form body and
        # the request as application/x-www-form-urlencoded.
        # This is how FCREPO4 accepts binary uploads.
        stream = request.stream
        # @FIXME Must decide what to do with this.
        mimetype = request.mimetype

    if mimetype == '' or mimetype == 'application/x-www-form-urlencoded':
        if getattr(stream, 'limit', 0) == 0:
            stream = mimetype = None
        else:
            mimetype = 'application/octet-stream'

    return stream, mimetype


def _tombstone_response(e, uid):
    headers = {
        'Link': '<{}/fcr:tombstone>; rel="hasTombstone"'.format(request.url),
    } if e.uid == uid else {}
    return str(e), 410, headers


def set_post_put_params():
    """
    Sets handling and content disposition for POST and PUT by parsing headers.
    """
    handling = 'strict'
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


def parse_repr_options(retr_opts):
    """
    Set options to retrieve IMR.

    Ideally, IMR retrieval is done once per request, so all the options
    are set once in the `imr()` property.

    :param dict retr_opts:: Options parsed from `Prefer` header.
    """
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


def _headers_from_metadata(rsrc):
    """
    Create a dict of headers from a metadata graph.

    :param lakesuperior.model.ldpr.Ldpr rsrc: Resource to extract metadata
        from.
    """
    out_headers = defaultdict(list)

    digest = rsrc.metadata.value(nsc['premis'].hasMessageDigest)
    if digest:
        etag = digest.identifier.split(':')[-1]
        etag_str = (
                'W/"{}"'.format(etag)
                if nsc['ldp'].RDFSource in rsrc.ldp_types
                else etag)
        out_headers['ETag'] = etag_str,

    last_updated_term = rsrc.metadata.value(nsc['fcrepo'].lastModified)
    if last_updated_term:
        out_headers['Last-Modified'] = arrow.get(last_updated_term)\
            .format('ddd, D MMM YYYY HH:mm:ss Z')

    for t in rsrc.ldp_types:
        out_headers['Link'].append(
                '{};rel="type"'.format(t.n3()))

    mimetype = rsrc.metadata.value(nsc['ebucore'].hasMimeType)
    if mimetype:
        out_headers['Content-Type'] = mimetype

    return out_headers

