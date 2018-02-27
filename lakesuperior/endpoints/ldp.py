import logging

from collections import defaultdict
from pprint import pformat
from functools import wraps
from uuid import uuid4

import arrow

from flask import (
        Blueprint, current_app, g, make_response, render_template,
        request, send_file)
from rdflib.namespace import XSD
from rdflib.term import Literal

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.exceptions import (ResourceNotExistsError, TombstoneError,
        ServerManagedTermError, InvalidResourceError, SingleSubjectError,
        ResourceExistsError, IncompatibleLdpTypeError)
from lakesuperior.model.generic_resource import PathSegment
from lakesuperior.model.ldp_factory import LdpFactory
from lakesuperior.model.ldp_nr import LdpNr
from lakesuperior.model.ldp_rs import LdpRs
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.store_layouts.ldp_rs.lmdb_store import LmdbStore, TxnManager
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
def instantiate_req_vars():
    g.store = current_app.rdfly.store
    g.tbox = Toolbox()


@ldp.before_request
def request_timestamp():
    g.timestamp = arrow.utcnow()
    g.timestamp_term = Literal(g.timestamp, datatype=XSD.dateTime)


@ldp.after_request
def log_request_end(rsp):
    logger.info('** End {} {} **\n\n'.format(request.method, request.url))

    return rsp


def transaction(write=False):
    '''
    Handle atomic operations in a store.

    This wrapper ensures that a write operation is performed atomically. It
    also takes care of sending a message for each resource changed in the
    transaction.
    '''
    def _transaction_deco(fn):
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            g.changelog = []
            store = current_app.rdfly.store
            if isinstance(store, LmdbStore):
                with TxnManager(store, write=write) as txn:
                    ret = fn(*args, **kwargs)
                return ret
            else:
                try:
                    ret = fn(*args, **kwargs)
                except:
                    logger.warn('Rolling back transaction.')
                    store.rollback()
                    raise
                else:
                    logger.info('Committing transaction.')
                    #if hasattr(store, '_edits'):
                    #    # @FIXME ugly.
                    #    self.rdfly._conn.optimize_edits()
                    store.commit()
                    return ret
            # @TODO re-enable, maybe leave out the delta part
            #for ev in g.changelog:
            #    #self._logger.info('Message: {}'.format(pformat(ev)))
            #    send_event_msg(*ev)

        return _wrapper
    return _transaction_deco


def send_msg(self, ev_type, remove_trp=None, add_trp=None):
    '''
    Sent a message about a changed (created, modified, deleted) resource.
    '''
    try:
        type = self.types
        actor = self.metadata.value(nsc['fcrepo'].createdBy)
    except (ResourceNotExistsError, TombstoneError):
        type = set()
        actor = None
        for t in add_trp:
            if t[1] == RDF.type:
                type.add(t[2])
            elif actor is None and t[1] == nsc['fcrepo'].createdBy:
                actor = t[2]

    g.changelog.append((set(remove_trp), set(add_trp), {
        'ev_type' : ev_type,
        'time' : g.timestamp,
        'type' : type,
        'actor' : actor,
    }))


## REST SERVICES ##

@ldp.route('/<path:uid>', methods=['GET'], strict_slashes=False)
@ldp.route('/', defaults={'uid': ''}, methods=['GET'], strict_slashes=False)
@ldp.route('/<path:uid>/fcr:metadata', defaults={'force_rdf' : True},
        methods=['GET'])
@transaction()
def get_resource(uid, force_rdf=False):
    '''
    Retrieve RDF or binary content.

    @param uid (string) UID of resource to retrieve. The repository root has
    an empty string for UID.
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
        rsrc = LdpFactory.from_stored(uid, repr_options)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        out_headers.update(rsrc.head())
        if (
                isinstance(rsrc, LdpRs)
                or isinstance(rsrc, PathSegment)
                or is_accept_hdr_rdf_parsable()
                or force_rdf):
            rsp = rsrc.get()
            return negotiate_content(rsp, out_headers)
        else:
            logger.info('Streaming out binary content.')
            rsp = make_response(send_file(rsrc.local_path, as_attachment=True,
                    attachment_filename=rsrc.filename, mimetype=rsrc.mimetype))
            rsp.headers['Link'] = '<{}/fcr:metadata>; rel="describedby"'\
                    .format(rsrc.uri)

            return rsp


@ldp.route('/<path:parent>', methods=['POST'], strict_slashes=False)
@ldp.route('/', defaults={'parent': ''}, methods=['POST'],
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
        with TxnManager(g.store, True):
            uid = LdpFactory.mint_uid(parent, slug)
            logger.debug('Generated UID for POST: {}'.format(uid))
            rsrc = LdpFactory.from_provided(
                    uid, content_length=request.content_length,
                    stream=stream, mimetype=mimetype, handling=handling,
                    disposition=disposition)
            rsrc.post()
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
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


@ldp.route('/<path:uid>/fcr:versions', methods=['GET'])
@transaction()
def get_version_info(uid):
    '''
    Get version info (`fcr:versions`).
    '''
    try:
        rsp = Ldpr(uid).get_version_info()
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return negotiate_content(rsp)


@ldp.route('/<path:uid>/fcr:versions/<ver_uid>', methods=['GET'])
@transaction()
def get_version(uid, ver_uid):
    '''
    Get an individual resource version.

    @param uid (string) Resource UID.
    @param ver_uid (string) Version UID.
    '''
    try:
        rsp = Ldpr(uid).get_version(ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return negotiate_content(rsp)


@ldp.route('/<path:uid>/fcr:versions', methods=['POST', 'PUT'])
@transaction(True)
def post_version(uid):
    '''
    Create a new resource version.
    '''
    if request.method == 'PUT':
        return 'Method not allowed.', 405
    ver_uid = request.headers.get('slug', None)
    try:
        ver_uri = LdpFactory.from_stored(uid).create_version(ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return '', 201, {'Location': ver_uri}


@ldp.route('/<path:uid>/fcr:versions/<ver_uid>', methods=['PATCH'])
@transaction(True)
def patch_version(uid, ver_uid):
    '''
    Revert to a previous version.

    NOTE: This creates a new version snapshot.

    @param uid (string) Resource UID.
    @param ver_uid (string) Version UID.
    '''
    try:
        LdpFactory.from_stored(uid).revert_to_version(ver_uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    else:
        return '', 204


@ldp.route('/<path:uid>', methods=['PUT'], strict_slashes=False)
@ldp.route('/<path:uid>/fcr:metadata', defaults={'force_rdf' : True},
        methods=['PUT'])
@transaction(True)
def put_resource(uid):
    '''
    Add a new resource at a specified URI.
    '''
    # Parse headers.
    logger.info('Request headers: {}'.format(request.headers))

    rsp_headers = {'Content-Type' : 'text/plain; charset=utf-8'}

    handling, disposition = set_post_put_params()
    stream, mimetype = bitstream_from_req()

    try:
        rsrc = LdpFactory.from_provided(
                uid, content_length=request.content_length,
                stream=stream, mimetype=mimetype, handling=handling,
                disposition=disposition)
        if not request.content_length and rsrc.is_stored:
            raise InvalidResourceError(rsrc.uid,
                'Resource {} already exists and no data set was provided.')
    except InvalidResourceError as e:
        return str(e), 409
    except (ServerManagedTermError, SingleSubjectError) as e:
        return str(e), 412
    except IncompatibleLdpTypeError as e:
        return str(e), 415

    try:
        ret = rsrc.put()
        rsp_headers.update(rsrc.head())
    except (InvalidResourceError, ResourceExistsError) as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uid)

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


@ldp.route('/<path:uid>', methods=['PATCH'], strict_slashes=False)
@transaction(True)
def patch_resource(uid):
    '''
    Update an existing resource with a SPARQL-UPDATE payload.
    '''
    rsp_headers = {'Content-Type' : 'text/plain; charset=utf-8'}
    rsrc = LdpRs(uid)
    if request.mimetype != 'application/sparql-update':
        return 'Provided content type is not a valid parsable format: {}'\
                .format(request.mimetype), 415

    try:
        rsrc.patch(request.get_data().decode('utf-8'))
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uid)
    except (ServerManagedTermError, SingleSubjectError) as e:
        return str(e), 412
    else:
        rsp_headers.update(rsrc.head())
        return '', 204, rsp_headers


@ldp.route('/<path:uid>/fcr:metadata', methods=['PATCH'])
@transaction(True)
def patch_resource_metadata(uid):
    return patch_resource(uid)


@ldp.route('/<path:uid>', methods=['DELETE'])
@transaction(True)
def delete_resource(uid):
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
        LdpFactory.from_stored(uid, repr_opts).delete(
                leave_tstone=leave_tstone)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uid)

    return '', 204, headers


@ldp.route('/<path:uid>/fcr:tombstone', methods=['GET', 'POST', 'PUT',
        'PATCH', 'DELETE'])
@transaction(True)
def tombstone(uid):
    '''
    Handle all tombstone operations.

    The only allowed methods are POST and DELETE; any other verb will return a
    405.
    '''
    logger.debug('Deleting tombstone for {}.'.format(uid))
    rsrc = Ldpr(uid)
    try:
        rsrc.metadata
    except TombstoneError as e:
        if request.method == 'DELETE':
            if e.uid == uid:
                rsrc.purge()
                return '', 204
            else:
                return _tombstone_response(e, uid)
        elif request.method == 'POST':
            if e.uid == uid:
                rsrc_uri = rsrc.resurrect()
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


def negotiate_content(rsp, headers=None):
    '''
    Return HTML or serialized RDF depending on accept headers.
    '''
    if request.accept_mimetypes.best == 'text/html':
        rsrc = rsp.resource(request.path)
        return render_template(
                'resource.html', rsrc=rsrc, nsm=nsm,
                blacklist = vw_blacklist)
    else:
        for p in vw_blacklist:
            rsp.remove((None, p, None))
        return (rsp.serialize(format='turtle'), headers)


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
    # @TODO This may change in favor of more low-level handling if the file
    # system is not local.
    return send_file(rsrc.local_path, as_attachment=True,
            attachment_filename=rsrc.filename)


def _tombstone_response(e, uid):
    headers = {
        'Link': '<{}/fcr:tombstone>; rel="hasTombstone"'.format(request.url),
    } if e.uid == uid else {}
    return str(e), 410, headers


def set_post_put_params():
    '''
    Sets handling and content disposition for POST and PUT by parsing headers.
    '''
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


