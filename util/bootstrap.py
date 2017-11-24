#!/usr/bin/env python

import os
import shutil
import sys
sys.path.append('.')

from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from lakesuperior.store_layouts.rdf.graph_store_connector import \
        GraphStoreConnector
from lakesuperior.model.ldpr import Ldpr

# This script will parse configuration files and initialize a filesystem and
# triplestore with an empty FCREPO repository.
# It is used in test suites and on a first run.
#
# Additional, scaffolding files may be parsed to create initial contents.


def bootstrap_db(app):
    '''
    Initialize RDF store.
    '''
    dbconf = app.config['store']['ldp_rs']
    print('Resetting RDF store to base data set: {}'.format(dbconf['webroot']))
    db = GraphStoreConnector(
            query_ep=dbconf['webroot'] + dbconf['query_ep'],
            update_ep=dbconf['webroot'] + dbconf['update_ep'],
            autocommit=True)

    # @TODO Make configurable.
    db.ds.default_context.parse(source='data/bootstrap/simple_layout.nq',
            format='nquads')

    return db


def bootstrap_binary_store(app):
    '''
    Initialize binary file store.
    '''
    root_path = app.config['store']['ldp_nr']['path']
    print('Removing binary store path: {}'.format(root_path))
    try:
        shutil.rmtree(root_path)
    except FileNotFoundError:
        pass
    os.makedirs(root_path + '/tmp')


if __name__=='__main__':
    sys.stdout.write(
            'This operation will WIPE ALL YOUR DATA. Are you sure? '
            '(Please type `yes` to continue) > ')
    choice = input().lower()
    if choice != 'yes':
        print('Aborting.')
        sys.exit()

    app = create_app(config['application'], config['logging'])
    bootstrap_db(app)
    bootstrap_binary_store(app)
