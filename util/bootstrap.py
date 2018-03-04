#!/usr/bin/env python

import os
import sys
sys.path.append('.')

import lakesuperior.env_setup

from lakesuperior.env import env
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager
from lakesuperior.model.ldpr import Ldpr

__doc__ = '''
This script will parse configuration files and initialize a filesystem and
triplestore with an empty FCREPO repository.
It is used in test suites and on a first run.

Additional scaffolding files may be parsed to create initial contents.
'''

sys.stdout.write(
        'This operation will WIPE ALL YOUR DATA. Are you sure? '
        '(Please type `yes` to continue) > ')
choice = input().lower()
if choice != 'yes':
    print('Aborting.')
    sys.exit()

with TxnManager(env.app_globals.rdf_store, write=True) as txn:
    env.app_globals.rdfly.bootstrap()
    env.app_globals.rdfly.store.close()

env.app_globals.nonrdfly.bootstrap()
