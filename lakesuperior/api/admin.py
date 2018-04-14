import logging

from lakesuperior import env
from lakesuperior.config_parser import parse_config
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

    :rtype: dict
    :return: Store statistics, resource statistics.
    """
    import lakesuperior.env_setup
    repo_stats = {'rsrc_stats': env.app_globals.rdfly.count_rsrc()}
    with TxnManager(env.app_globals.rdf_store) as txn:
        repo_stats['store_stats'] = env.app_globals.rdf_store.stats()

    return repo_stats


def migrate(src, dest, start_pts=None, list_file=None, **kwargs):
    """
    Migrate an LDP repository to a new LAKEsuperior instance.

    See :py:meth:`Migrator.__init__`.
    """
    if start_pts:
        if not isinstance(
                start_pts, list) and not isinstance(start_pts, tuple):
            start_pts = (start_pts,)
    elif not list_file:
        start_pts = ('/',)

    return Migrator(src, dest, **kwargs).migrate(start_pts, list_file)


def integrity_check():
    """
    Check integrity of the data set.

    At the moment this is limited to referential integrity. Other checks can
    be added and triggered by different argument flags.
    """
    with TxnManager(env.app_globals.rdfly.store):
        return set(env.app_globals.rdfly.find_refint_violations())
