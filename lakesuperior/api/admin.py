import logging

import lmdb

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


def dump(src, dest, start='/', binary_handling='include'):
    '''
    Dump a whole LDP repository or parts of it to disk.

    @param src (rdflib.term.URIRef) Webroot of source repository. This must
    correspond to the LDP root node (for Fedora it can be e.g.
    `http://localhost:8080fcrepo/rest/`) and is used to determine if URIs
    retrieved are managed by this repository.
    @param dest (rdflib.URIRef) Base URI of the destination. This can be any
    container in a LAKEsuperior server. If the resource exists, it must be an
    LDP container. If it does not exist, it will be created.
    @param start (tuple|list) List of starting points to retrieve resources
    from. It would typically be the repository root in case of a full dump
    or one or more resources in the repository for a partial one.
    @binary_handling (string) One of 'include', 'truncate' or 'split'.
    '''
    # 1. Retrieve list of resources.
    if not isinstance(start, list) and not isinstance(start, tuple):
        start = (start,)
    subjects = _gather_subjects(src, start)


def _gather_subjects(webroot, start_pts):
    env = lmdb.open('/var/tmp/
    for start in start_pts:
        if not start.startswith('/'):
            raise ValueError('Starting point {} does not begin with a slash.'
                    .format(start))

        pfx = src.rstrip('/') + start
