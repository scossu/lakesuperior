import io
import json
import os.path

from flask import Flask

from lakesuperior.config_parser import config
from lakesuperior.ldp.resource import Resource

app = Flask(__name__)
app.config.update(config['flask'])

@app.route('/', methods=['GET'])
def index():
    '''Homepage'''
    return 'Hello. This is LAKEsuperior.'


@app.route('/<uuid>', methods=['GET'])
def get_resource():
    '''Add a new resource in a new URI.'''
    rsrc = Resource.get(uuid)
    return rsrc.path


@app.route('/<path>', methods=['POST'])
def post_resource():
    '''Add a new resource in a new URI.'''
    rsrc = Resource.post(path)
    return rsrc.path


@app.route('/<path>', methods=['PUT'])
def put_resource():
    '''Add a new resource in a new URI.'''
    rsrc = Resource.put(path)
    return rsrc.path


