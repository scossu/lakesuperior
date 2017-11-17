import logging
import os

from logging.config import dictConfig

from flask import Flask

from lakesuperior.endpoints.ldp import ldp
from lakesuperior.endpoints.query import query


# App factory.

def create_app(app_conf, logging_conf):
    app = Flask(__name__)
    app.config.update(app_conf)

    dictConfig(logging_conf)
    logger = logging.getLogger(__name__)
    logger.info('Starting LAKEsuperior HTTP server.')

    ## Configure endpoint blueprints here. ##

    app.register_blueprint(ldp, url_prefix='/ldp', url_defaults={
        'url_prefix': 'ldp'
    })
    # Legacy endpoint. @TODO Deprecate.
    app.register_blueprint(ldp, url_prefix='/rest', url_defaults={
        'url_prefix': 'rest'
    })
    app.register_blueprint(query, url_prefix='/query')

    return app


