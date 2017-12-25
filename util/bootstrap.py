#!/usr/bin/env python

import os
import shutil
import sys
sys.path.append('.')

from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from lakesuperior.store_layouts.ldp_rs.bdb_connector import \
        BdbConnector
from lakesuperior.model.ldpr import Ldpr

__doc__ = '''
This script will parse configuration files and initialize a filesystem and
triplestore with an empty FCREPO repository.
It is used in test suites and on a first run.

Additional, scaffolding files may be parsed to create initial contents.
'''


def bootstrap_db(app):
    '''
    Initialize RDF store.
    '''
    print('Cleaning up graph store: {}'.format(
            app.config['store']['ldp_rs']['connector']['options']['location']))
    for g in app.rdfly.ds.graphs():
        app.rdfly.ds.remove_graph(g)

    # @TODO Make configurable.
    print('Populating graph store with base dataset.')
    app.rdfly.ds.default_context.parse(
            source='data/bootstrap/default_layout.nq', format='nquads')

    app.rdfly.ds.store.commit()
    app.rdfly.ds.close()

    return app.rdfly


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
    print('Recreating binary store path: {}'.format(root_path))
    os.makedirs(root_path + '/tmp')
    print('Binary store initialized.')


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
