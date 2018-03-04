import logging

from flask import Flask

from lakesuperior.endpoints.admin import admin
from lakesuperior.endpoints.ldp import ldp
from lakesuperior.endpoints.main import main
from lakesuperior.endpoints.query import query

logger = logging.getLogger(__name__)


def create_app(app_conf):
    '''
    App factory.

    Create a Flask app.

    @param app_conf (dict) Configuration parsed from `application.yml` file.
    '''
    app = Flask(__name__)
    app.config.update(app_conf)

    logger.info('Starting LAKEsuperior HTTP server.')

    app.register_blueprint(main)
    app.register_blueprint(ldp, url_prefix='/ldp', url_defaults={
        'url_prefix': 'ldp'
    })
    # Legacy endpoint. @TODO Deprecate.
    app.register_blueprint(ldp, url_prefix='/rest', url_defaults={
        'url_prefix': 'rest'
    })
    app.register_blueprint(query, url_prefix='/query')
    app.register_blueprint(admin, url_prefix='/admin')

    return app


