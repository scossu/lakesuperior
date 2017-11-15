import logging
import os

from logging.config import dictConfig

from flask import Flask, render_template

from lakesuperior.config_parser import config
from lakesuperior.endpoints.ldp import ldp
from lakesuperior.endpoints.query import query

fcrepo = Flask(__name__)
fcrepo.config.update(config['flask'])

dictConfig(config['logging'])
logger = logging.getLogger(__name__)
logger.info('Starting LAKEsuperior HTTP server.')

## Configure enpoint blueprints here. ##

fcrepo.register_blueprint(ldp, url_prefix='/ldp', url_defaults={
    'url_prefix': 'ldp'
})
# Legacy endpoint. @TODO Deprecate.
fcrepo.register_blueprint(ldp, url_prefix='/rest', url_defaults={
    'url_prefix': 'rest'
})
fcrepo.register_blueprint(query, url_prefix='/query')

# Initialize temporary folders.
tmp_path = config['application']['store']['ldp_nr']['path'] + '/tmp'
if not os.path.exists(tmp_path):
    os.makedirs(tmp_path)


## ROUTES ##

@fcrepo.route('/', methods=['GET'])
def index():
    '''
    Homepage.
    '''
    return render_template('index.html')


@fcrepo.route('/debug', methods=['GET'])
def debug():
    '''
    Debug page.
    '''
    raise RuntimeError()

