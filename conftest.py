import sys
sys.path.append('.')
import numpy
import random
import uuid

import pytest

from PIL import Image

from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from util.generators import random_image
from util.bootstrap import bootstrap_binary_store


@pytest.fixture(scope='module')
def app():
    app = create_app(config['test'], config['logging'])

    yield app


@pytest.fixture(scope='module')
def db(app):
    '''
    Set up and tear down test triplestore.
    '''
    db = app.rdfly
    db.bootstrap()
    bootstrap_binary_store(app)

    yield db

    print('Tearing down fixure graph store.')
    for g in db.ds.graphs():
        db.ds.remove_graph(g)

    db.ds.store.commit()


@pytest.fixture
def rnd_img():
    '''
    Generate a square image with random color tiles.
    '''
    return random_image(8, 256)


