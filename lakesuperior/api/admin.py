import logging

from lakesuperior.env import env
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager

__doc__ = '''
Admin API.

This module contains maintenance utilities and stats.
'''

logger = logging.getLogger(__name__)
app_globals = env.app_globals


def stats():
    '''
    Get repository statistics.

    @return dict Store statistics, resource statistics.
    '''
    repo_stats = {'rsrc_stats': env.app_globals.rdfly.count_rsrc()}
    with TxnManager(env.app_globals.rdf_store) as txn:
        repo_stats['store_stats'] = env.app_globals.rdf_store.stats()

    return repo_stats

