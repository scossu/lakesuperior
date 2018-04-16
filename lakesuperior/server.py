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

logger.info('Graph store location: {}'.format(
    env.app_globals.rdfly.config['location']))
logger.info('Binary store location: {}'.format(env.app_globals.nonrdfly.root))

fcrepo = create_app(env.app_globals.config['application'])

if __name__ == "__main__":
    fcrepo.run(host='0.0.0.0')
