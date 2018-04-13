import logging
from logging.config import dictConfig
from werkzeug.contrib.profiler import ProfilerMiddleware

# Environment must be set before importing the app factory function.
import lakesuperior.env_setup

from lakesuperior import env
from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals

options = {
    'restrictions': [30],
    #'profile_dir': '/tmp/lsup_profiling'
}

from lakesuperior.app import create_app

def run():
    fcrepo = create_app(config['application'])
    fcrepo.wsgi_app = ProfilerMiddleware(fcrepo.wsgi_app, **options)
    fcrepo.config['PROFILE'] = True
    fcrepo.run(debug = True)

if __name__ == '__main__':
    run()
