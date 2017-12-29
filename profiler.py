from werkzeug.contrib.profiler import ProfilerMiddleware

from lakesuperior.app import create_app
from lakesuperior.config_parser import config


fcrepo = create_app(config['application'], config['logging'])

options = {
    'restrictions': [30],
    'profile_dir': '/tmp/lsup_profiling'
}
fcrepo.wsgi_app = ProfilerMiddleware(fcrepo.wsgi_app, **options)
fcrepo.config['PROFILE'] = True
fcrepo.run(debug = True)

