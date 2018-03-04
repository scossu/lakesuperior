#!/usr/bin/env python

from werkzeug.contrib.profiler import ProfilerMiddleware

from lakesuperior.app import create_app
from lakesuperior.config_parser import config

options = {
    'restrictions': [30],
    #'profile_dir': '/tmp/lsup_profiling'
}

if __name__ == '__main__':
    fcrepo = create_app(config['application'])
    fcrepo.wsgi_app = ProfilerMiddleware(fcrepo.wsgi_app, **options)
    fcrepo.config['PROFILE'] = True
    fcrepo.run(debug = True)

