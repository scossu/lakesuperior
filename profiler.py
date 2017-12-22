from werkzeug.contrib.profiler import ProfilerMiddleware

from lakesuperior.app import create_app
from lakesuperior.config_parser import config


fcrepo = create_app(config['application'], config['logging'])

fcrepo.wsgi_app = ProfilerMiddleware(fcrepo.wsgi_app, restrictions=[30])
fcrepo.config['PROFILE'] = True
fcrepo.run(debug = True)

