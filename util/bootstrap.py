#!/usr/bin/env python

import os
import shutil
import sys
sys.path.append('.')

from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager
from lakesuperior.model.ldpr import Ldpr

__doc__ = '''
This script will parse configuration files and initialize a filesystem and
triplestore with an empty FCREPO repository.
It is used in test suites and on a first run.

Additional, scaffolding files may be parsed to create initial contents.
'''


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
    if hasattr(app.rdfly.store, 'begin'):
        with TxnManager(app.rdfly.store, write=True) as txn:
            app.rdfly.bootstrap()
            app.rdfly.store.close()
    else:
        app.rdfly.bootstrap()

    bootstrap_binary_store(app)
