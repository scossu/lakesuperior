import logging
import pytest

from os import makedirs, path
from shutil import rmtree
from tempfile import gettempdir

from lakesuperior import env
from lakesuperior.config_parser import parse_config
from lakesuperior.globals import AppGlobals
from lakesuperior.util.generators import random_image


# Override data directory locations.
config = parse_config()
data_dir = path.join(gettempdir(), 'lsup_test', 'data')
config['application']['data_dir'] = data_dir
config['application']['store']['ldp_nr']['location'] = (
        path.join(data_dir, 'ldpnr_store'))
config['application']['store']['ldp_rs']['location'] = (
        path.join(data_dir, 'ldprs_store'))

env.app_globals = AppGlobals(config)
from lakesuperior.app import create_app


@pytest.fixture(scope='module')
def app():
    app = create_app(env.app_globals.config['application'])

    yield app


@pytest.fixture(scope='module')
def db(app):
    '''
    Set up and tear down test triplestore.
    '''
    env.app_globals.rdfly.bootstrap()
    env.app_globals.nonrdfly.bootstrap()
    print('Initialized data store.')
    env.app_globals.rdf_store.open()
    env.app_globals.metadata_store.open()

    yield env.app_globals.rdfly

    # TODO improve this by using tempfile.TemporaryDirectory as a context
    # manager.
    print('Removing fixture data directory.')
    env.app_globals.metadata_store.close()
    env.app_globals.metadata_store.destroy()
    env.app_globals.rdf_store.close()
    env.app_globals.rdf_store.destroy()


@pytest.fixture
def rnd_img():
    '''
    Generate a square image with random color tiles.
    '''
    return random_image(8, 256)


@pytest.fixture(autouse=True)
def disable_logging():
    """Disable logging in all tests."""
    logging.disable(logging.INFO)
