import io
import json
import os.path
import pickle

import arrow

from hashlib import sha1
from uuid import  uuid4

from flask import Flask, request, url_for

from lakesuperior.config_parser import config
from lakesuperior.ldp.ldpr import Ldpr, Ldpc, LdpNr

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
    rsrc = Ldpc(uuid).get()
    if rsrc:
        headers = {
            #'ETag' : 'W/"{}"'.format(ret.value(nsc['premis
        }
        return (rsrc.graph.serialize(format='turtle'), headers)
    else:
        return ('Resource not found in repository: {}'.format(uuid), 404)


@app.route('/rest/<path:parent>', methods=['POST'])
@app.route('/rest/', defaults={'parent': None}, methods=['POST'])
def post_resource(parent):
    '''
    Add a new resource in a new URI.
    '''
    uuid = uuid4()

    uuid = '{}/{}'.format(parent, uuid) \
            if path else uuid
    rsrc = Ldpc(path).post(request.get_data().decode('utf-8'))

    return rsrc.uri, 201


@app.route('/rest/<path:uuid>', methods=['PUT'])
def put_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    rsrc = Ldpc(uuid).put(request.get_data().decode('utf-8'))
    return '', 204


@app.route('/rest/<path:uuid>', methods=['PATCH'])
def patch_resource(uuid):
    '''
    Add a new resource at a specified URI.
    '''
    rsrc = Ldpc(uuid).patch(request.get_data().decode('utf-8'))
    return '', 204


@app.route('/rest/<path:uuid>', methods=['DELETE'])
def delete_resource(uuid):
    '''
    Delete a resource.
    '''
    rsrc = Ldpc(uuid).delete()
    return '', 204
