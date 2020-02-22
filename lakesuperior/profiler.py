import logging
from logging.config import dictConfig
from werkzeug.contrib.profiler import ProfilerMiddleware

# Environment must be set before importing the app factory function.
from lakesuperior import env
env.setup()

options = {
    'restrictions': [50],
    #'profile_dir': '/var/tmp/lsup_profiling'
}

from lakesuperior.app import create_app

def run():
    fcrepo = create_app(env.app_globals.config['application'])
    fcrepo.wsgi_app = ProfilerMiddleware(fcrepo.wsgi_app, **options)
    fcrepo.config['PROFILE'] = True
    fcrepo.run(debug = True)

if __name__ == '__main__':
    run()
