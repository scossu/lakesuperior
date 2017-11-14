import logging

from collections import defaultdict
from uuid import uuid4

from flask import Blueprint, request, send_file
from rdflib import Graph
from werkzeug.datastructures import FileStorage

from lakesuperior.exceptions import InvalidResourceError, \
        ResourceExistsError, ResourceNotExistsError, ServerManagedTermError, \
        TombstoneError
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.model.ldp_nr import LdpNr
from lakesuperior.model.ldp_rs import Ldpc, LdpDc, LdpIc, LdpRs
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout
from lakesuperior.util.translator import Translator


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

## REST SERVICES ##

@ldp.route('/<path:uuid>', methods=['GET'])
@ldp.route('/', defaults={'uuid': None}, methods=['GET'],
        strict_slashes=False)
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
        prefer = Translator.parse_rfc7240(request.headers['prefer'])
        logger.debug('Parsed Prefer header: {}'.format(prefer))
        if 'return' in prefer:
            repr_options = prefer['return']

    try:
        rsrc = Ldpr.readonly_inst(uuid, repr_options)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)
    else:
        out_headers.update(rsrc.head())
        if isinstance(rsrc, LdpRs) \
                or request.headers['accept'] in accept_rdf \
                or force_rdf:
            return (rsrc.imr.graph.serialize(format='turtle'), out_headers)
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

    cls, data = class_from_req_body()

    try:
       rsrc = cls.inst_for_post(parent, slug)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409
    except TombstoneError as e:
        return _tombstone_response(e, uuid)

    if cls == LdpNr:
        try:
            cont_disp = Translator.parse_rfc7240(
                    request.headers['content-disposition'])
        except KeyError:
            cont_disp = None

        rsrc.post(data, mimetype=request.content_type, disposition=cont_disp)
    else:
        try:
            rsrc.post(data)
        except ServerManagedTermError as e:
            return str(e), 412

    out_headers.update({
        'Location' : rsrc.uri,
    })

    return rsrc.uri, out_headers, 201


@ldp.route('/<path:uuid>', methods=['PUT'])
def put_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    logger.info('Request headers: {}'.format(request.headers))
    rsp_headers = std_headers

    cls, data = class_from_req_body()

    rsrc = cls(uuid)

    # Parse headers.
    pref_handling = None
    if cls == LdpNr:
        try:
            logger.debug('Headers: {}'.format(request.headers))
            cont_disp = Translator.parse_rfc7240(
                    request.headers['content-disposition'])
        except KeyError:
            cont_disp = None

        try:
            ret = rsrc.put(data, disposition=cont_disp)
        except InvalidResourceError as e:
            return str(e), 409
        except ResourceExistsError as e:
            return str(e), 409
        except TombstoneError as e:
            return _tombstone_response(e, uuid)
    else:
        if 'prefer' in request.headers:
            prefer = Translator.parse_rfc7240(request.headers['prefer'])
            logger.debug('Parsed Prefer header: {}'.format(prefer))
            if 'handling' in prefer:
                pref_handling = prefer['handling']['value']

        try:
            ret = rsrc.put(data, handling=pref_handling)
        except InvalidResourceError as e:
            return str(e), 409
        except ResourceExistsError as e:
            return str(e), 409
        except TombstoneError as e:
            return _tombstone_response(e, uuid)
        except ServerManagedTermError as e:
            return str(e), 412

    res_code = 201 if ret == BaseRdfLayout.RES_CREATED else 204
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
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404
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
    rsrc = Ldpc(uuid)

    try:
        rsrc.delete()
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404
    except TombstoneError as e:
        return _tombstone_response(e, uuid)

    return '', 204, headers


def class_from_req_body():
    '''
    Determine LDP type (and instance class) from the provided RDF body.
    '''
    logger.debug('Content type: {}'.format(request.mimetype))
    logger.debug('files: {}'.format(request.files))
    logger.debug('stream: {}'.format(request.stream))

    # LDP-NR types
    if  not request.data:
        logger.debug('No data received in body.')
        cls = Ldpc
        data = None
    elif request.mimetype in accept_rdf:
        # Parse out the RDF string.
        data = request.data.decode('utf-8')
        g = Graph().parse(data=data, format=request.mimetype)

        if Ldpr.MBR_RSRC_URI in g.predicates() and \
                Ldpr.MBR_REL_URI in g.predicates():
            if Ldpr.INS_CNT_REL_URI in g.predicates():
                cls = LdpIc
            else:
                cls = LdpDc
        else:
            cls = Ldpc
    else:
        cls = LdpNr
        if request.mimetype == 'multipart/form-data':
            # This seems the "right" way to upload a binary file, with a
            # multipart/form-data MIME type and the file in the `file`
            # field. This however is not supported by FCREPO4.
            data = request.files.get('file').stream
        else:
            # This is a less clean way, with the file in the form body and
            # the request as application/x-www-form-urlencoded.
            # This is how FCREPO4 accepts binary uploads.
            data = request.stream

    logger.info('Creating resource of type: {}'.format(cls.__name__))

    return cls, data


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
