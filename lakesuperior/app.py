import logging
import os

from importlib import import_module
from logging.config import dictConfig

from flask import Flask

from lakesuperior.endpoints.ldp import ldp
from lakesuperior.messaging.messenger import Messenger
from lakesuperior.endpoints.query import query
from lakesuperior.toolbox import Toolbox


# App factory.

def create_app(app_conf, logging_conf):
    '''
    App factory.

    Create a Flask app with a given configuration and initialize persistent
    connections.

    @param app_conf (dict) Configuration parsed from `application.yml` file.
    @param logging_conf (dict) Logging configuration from `logging.yml` file.
    '''
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

    # Initialize RDF and file store.
    def load_layout(type):
        layout_cls = app_conf['store'][type]['layout']
        store_mod = import_module('lakesuperior.store_layouts.{0}.{1}'.format(
                type, layout_cls))
        layout_cls = getattr(store_mod, camelcase(layout_cls))

        return layout_cls(app_conf['store'][type])

    app.rdfly = load_layout('ldp_rs')
    app.nonrdfly = load_layout('ldp_nr')

    # Set up messaging.
    app.messenger = Messenger(app_conf['messaging'])

    return app


def camelcase(word):
    '''
    Convert a string with underscores with a camel-cased one.

    Ripped from https://stackoverflow.com/a/6425628
    '''
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


