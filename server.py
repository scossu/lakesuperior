#!/usr/bin/env python

import logging
from logging.config import dictConfig

# Environment must be set before importing the app factory function.
import lakesuperior.env_setup

from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals
from lakesuperior.env import env

#import threading
#logger = logging.getLogger(__name__)
#logger.debug('In main: {}'.format(threading.current_thread()), stack_info=True)
#print('Env: {}'.format(env.__dict__))

from lakesuperior.app import create_app

dictConfig(env.config['logging'])

fcrepo = create_app(env.config['application'])

if __name__ == "__main__":
    fcrepo.run(host='0.0.0.0')
