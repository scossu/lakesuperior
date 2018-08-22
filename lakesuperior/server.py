import logging

from logging.config import dictConfig

# Environment must be set before importing the app factory function.
import lakesuperior.env_setup

from lakesuperior import env
from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals

from lakesuperior.app import create_app

dictConfig(env.app_globals.config['logging'])
logger = logging.getLogger(__name__)

# this stays at the module level so it's used by GUnicorn.
fcrepo = create_app(env.app_globals.config['application'])

def run():
    fcrepo.run(host='0.0.0.0')

if __name__ == "__main__":
    run()
