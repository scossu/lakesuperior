import io
import json
import os.path
import pickle

import arrow

from hashlib import sha1
from uuid import  uuid4

from flask import Flask, request, url_for

from lakesuperior.config_parser import config
from lakesuperior.ldp.ldpr import Ldpr, Ldpc, LdpNr, \
        InvalidResourceError, ResourceNotExistsError

app = Flask(__name__)
app.config.update(config['flask'])


## ROUTES ##

@app.route('/', methods=['GET'])
def index():
    '''
    Homepage.
    '''
    return 'Hello. This is LAKEsuperior.'


@app.route('/debug', methods=['GET'])
def debug():
    '''
    Debug page.
    '''
    raise RuntimeError()


## REST SERVICES ##

@app.route('/rest/<path:uuid>', methods=['GET'])
@app.route('/rest/', defaults={'uuid': None}, methods=['GET'])
def get_resource(uuid):
    '''
    Retrieve RDF or binary content.
    '''
    # @TODO Add conditions for LDP-NR
    rsrc = Ldpc(uuid)
    try:
        out = rsrc.get()
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404
    else:
        headers = rsrc.head()
        return (out.graph.serialize(format='turtle'), headers)


@app.route('/rest/<path:parent>', methods=['POST'])
@app.route('/rest/', defaults={'parent': None}, methods=['POST'])
def post_resource(parent):
    '''
    Add a new resource in a new URI.
    '''
    try:
       rsrc = Ldpc.inst_for_post(parent, request.headers['Slug'] or None)
    except ResourceNotExistsError as e:
        return str(e), 404
    except InvalidResourceError as e:
        return str(e), 409

    rsrc.post(request.get_data().decode('utf-8'))

    headers = {
        'Location' : rsrc.uri
    }

    return rsrc.uri, headers, 201


@app.route('/rest/<path:uuid>', methods=['PUT'])
def put_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    rsrc = Ldpc(uuid)

    rsrc.put(request.get_data().decode('utf-8'))
    return '', 204


@app.route('/rest/<path:uuid>', methods=['PATCH'])
def patch_resource(uuid):
    '''
    Update an existing resource with a SPARQL-UPDATE payload.
    '''
    rsrc = Ldpc(uuid)

    try:
        rsrc.patch(request.get_data().decode('utf-8'))
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404

    return '', 204


@app.route('/rest/<path:uuid>', methods=['DELETE'])
def delete_resource(uuid):
    '''
    Delete a resource.
    '''
    rsrc = Ldpc(uuid)

    try:
        rsrc.delete()
    except ResourceNotExistsError:
        return 'Resource #{} not found.'.format(rsrc.uuid), 404

    return '', 204
