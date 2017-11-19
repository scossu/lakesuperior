import io
import sys
sys.path.append('.')
import numpy
import random
import uuid

from hashlib import sha1

import pytest

from PIL import Image

from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from lakesuperior.store_layouts.rdf.graph_store_connector import \
        GraphStoreConnector


@pytest.fixture(scope='module')
def app():
    app = create_app(config['test'], config['logging'])

    yield app


@pytest.fixture(scope='module')
def db(app):
    '''
    Set up and tear down test triplestore.
    '''
    dbconf = app.config['store']['ldp_rs']
    db = GraphStoreConnector(
            query_ep=dbconf['webroot'] + dbconf['query_ep'],
            update_ep=dbconf['webroot'] + dbconf['update_ep'])

    db.ds.default_context.parse(source='data/bootstrap/simple_layout.nq',
            format='nquads')
    db.store.commit()

    yield db

    print('Tearing down fixure graph store.')
    for g in db.ds.graphs():
        db.ds.remove_graph(g)
    db.store.commit()


@pytest.fixture
def rnd_image(rnd_utf8_string):
    '''
    Generate a square image with random color tiles.
    '''
    ts = 8 # Tile width and height. @TODO parametrize.
    ims = 256 # Image width and height. @TODO parametrize.
    imarray = numpy.random.rand(ts, ts, 3) * 255
    im = Image.fromarray(imarray.astype('uint8')).convert('RGBA')
    im = im.resize((ims, ims), Image.NEAREST)

    imf = io.BytesIO()
    im.save(imf, format='png')
    imf.seek(0)
    hash = sha1(imf.read()).hexdigest()

    return {
        'content' : imf,
        'hash' : hash,
        'filename' : rnd_utf8_string + '.png'
    }


@pytest.fixture
def rnd_utf8_string():
    '''
    Generate a random UTF-8 string.
    '''
    # @TODO Update this to include code point ranges to be sampled
    include_ranges = [
        ( 0x0021, 0x0021 ),
        ( 0x0023, 0x0026 ),
        ( 0x0028, 0x007E ),
        ( 0x00A1, 0x00AC ),
        ( 0x00AE, 0x00FF ),
        ( 0x0100, 0x017F ),
        ( 0x0180, 0x024F ),
        ( 0x2C60, 0x2C7F ),
        ( 0x16A0, 0x16F0 ),
        ( 0x0370, 0x0377 ),
        ( 0x037A, 0x037E ),
        ( 0x0384, 0x038A ),
        ( 0x038C, 0x038C ),
    ]
    length = 64 # String length. @TODO parametrize.
    alphabet = [
        chr(code_point) for current_range in include_ranges
            for code_point in range(current_range[0], current_range[1] + 1)
    ]
    return ''.join(random.choice(alphabet) for i in range(length))




