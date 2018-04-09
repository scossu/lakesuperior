import pytest

from lakesuperior import env
from lakesuperior.config_parser import test_config
from lakesuperior.globals import AppGlobals

env.config = test_config
env.app_globals = AppGlobals(test_config)
from lakesuperior.app import create_app
from lakesuperior.util.generators import random_image

env.config = test_config

@pytest.fixture(scope='module')
def app():
    app = create_app(env.config['application'])

    yield app


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


