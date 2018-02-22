import logging
import os

from importlib import import_module
from logging.config import dictConfig

from flask import Flask

from lakesuperior.endpoints.admin import admin
from lakesuperior.endpoints.ldp import ldp
from lakesuperior.endpoints.main import main
from lakesuperior.endpoints.query import query
from lakesuperior.messaging.messenger import Messenger
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

    # Initialize RDF store connector.
    conn_mod_name = app_conf['store']['ldp_rs']['connector']['module']
    conn_mod = import_module('lakesuperior.store_layouts.ldp_rs.{}'.format(
            conn_mod_name))
    conn_cls = getattr(conn_mod, camelcase(conn_mod_name))
    rdf_store_conn = conn_cls(
            **app_conf['store']['ldp_rs']['connector']['options'])
    logger.info('RDF store: {}'.format(conn_mod_name))

    # Initialize RDF layout.
    rdfly_mod_name = app_conf['store']['ldp_rs']['layout']
    rdfly_mod = import_module('lakesuperior.store_layouts.ldp_rs.{}'.format(
            rdfly_mod_name))
    rdfly_cls = getattr(rdfly_mod, camelcase(rdfly_mod_name))
    app.rdfly = rdfly_cls(rdf_store_conn, app_conf['store']['ldp_rs'])
    logger.info('RDF layout: {}'.format(rdfly_mod_name))

    # Initialize file layout.
    nonrdfly_mod_name = app_conf['store']['ldp_nr']['layout']
    nonrdfly_mod = import_module('lakesuperior.store_layouts.ldp_nr.{}'.format(
            nonrdfly_mod_name))
    nonrdfly_cls = getattr(nonrdfly_mod, camelcase(nonrdfly_mod_name))
    app.nonrdfly = nonrdfly_cls(app_conf['store']['ldp_nr'])
    logger.info('Non-RDF layout: {}'.format(nonrdfly_mod_name))

    # Set up messaging.
    app.messenger = Messenger(app_conf['messaging'])

    return app


def camelcase(word):
    '''
    Convert a string with underscores with a camel-cased one.

    Ripped from https://stackoverflow.com/a/6425628
    '''
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


