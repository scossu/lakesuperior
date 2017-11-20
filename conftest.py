import sys
sys.path.append('.')
import numpy
import random
import uuid

import pytest

from PIL import Image

from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from lakesuperior.store_layouts.rdf.graph_store_connector import \
        GraphStoreConnector
from util.generators import random_image


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
def rnd_img():
    '''
    Generate a square image with random color tiles.
    '''
    return random_image(8, 256)


