import sys

import pytest

from os import path
from shutil import rmtree
from tempfile import gettempdir

from lakesuperior import env_setup, env
from lakesuperior.app import create_app
from lakesuperior.util.generators import random_image

@pytest.fixture(scope='module')
def app():
    # Override data directory locations.
    data_dir = path.join(gettempdir(), 'lsup_test', 'data')
    env.config['application']['data_dir'] = data_dir
    env.config['application']['store']['ldp_nr']['location'] = path.join(
            data_dir, 'ldpnr_store')
    env.config['application']['store']['ldp_rs']['location'] = path.join(
            data_dir, 'ldprs_store')
    app = create_app(env.config['application'])

    yield app

    # TODO improve this by using tempfile.TemporaryDirectory as a context
    # manager.
    print('Removing fixture data directory.')
    rmtree(data_dir)


@pytest.fixture(scope='module')
def db(app):
    '''
    Set up and tear down test triplestore.
    '''
    rdfly = env.app_globals.rdfly
    rdfly.bootstrap()
    env.app_globals.nonrdfly.bootstrap()

    yield rdfly

    print('Tearing down fixture graph store.')
    rdfly.store.destroy(rdfly.store.path)


@pytest.fixture
def rnd_img():
    '''
    Generate a square image with random color tiles.
    '''
    return random_image(8, 256)


