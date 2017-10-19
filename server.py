from flask import Flask

from lakesuperior.config_parser import config
from lakesuperior.endpoints.ldp import ldp
from lakesuperior.endpoints.query import query


fcrepo = Flask(__name__)
fcrepo.config.update(config['flask'])


## Configure enpoint blueprints here. ##

fcrepo.register_blueprint(ldp, url_prefix='/ldp')
# Legacy endpoint. @TODO Deprecate.
fcrepo.register_blueprint(ldp, url_prefix='/rest')
fcrepo.register_blueprint(query, url_prefix='/query')


## ROUTES ##

@fcrepo.route('/', methods=['GET'])
def index():
    '''
    Homepage.
    '''
    return u'<h1>Hello. This is LAKEsuperior.</h1><p>Exciting, isn’t it?</p>'


@fcrepo.route('/debug', methods=['GET'])
def debug():
    '''
    Debug page.
    '''
    raise RuntimeError()


