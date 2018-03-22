import logging

from lakesuperior.env import env
from lakesuperior.migrator import Migrator
from lakesuperior.store.ldp_nr.default_layout import DefaultLayout as FileLayout
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager

__doc__ = """
Admin API.

This module contains maintenance utilities and stats.
"""

logger = logging.getLogger(__name__)


def stats():
    """
    Get repository statistics.

    @return dict Store statistics, resource statistics.
    """
    import lakesuperior.env_setup
    repo_stats = {'rsrc_stats': env.app_globals.rdfly.count_rsrc()}
    with TxnManager(env.app_globals.rdf_store) as txn:
        repo_stats['store_stats'] = env.app_globals.rdf_store.stats()

    return repo_stats


def migrate(src, dest, start=('/',), **kwargs):
    """
    Migrate an LDP repository to a new LAKEsuperior instance.

    See :py:meth:`Migrator.__init__`.
    """
    # 1. Retrieve list of resources.
    start_pts = (
            (start,)
            if not isinstance(start, list) and not isinstance(start, tuple)
            else start)

    return Migrator(src, dest, start_pts, **kwargs).migrate()



