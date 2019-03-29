import hashlib
import logging

from lakesuperior import env
from lakesuperior.config_parser import parse_config
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import (
        ChecksumValidationError, IncompatibleLdpTypeError)
from lakesuperior.migrator import Migrator
from lakesuperior.store.ldp_nr.default_layout import DefaultLayout as FileLayout

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
    with env.app_globals.rdf_store.txn_ctx():
        repo_stats = {
            'rsrc_stats': env.app_globals.rdfly.count_rsrc(),
            'store_stats': env.app_globals.rdf_store.stats(),
            'nonrdf_stats': {
                'ct': env.app_globals.nonrdfly.file_ct,
                'size': env.app_globals.nonrdfly.store_size,
            },
        }

    return repo_stats


def migrate(src, dest, start_pts=None, list_file=None, **kwargs):
    """
    Migrate an LDP repository to a new Lakesuperior instance.

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
    with env.app_globals.rdfly.store.txn_ctx():
        return set(env.app_globals.rdfly.find_refint_violations())


def fixity_check(uid):
    """
    Check fixity of a resource.

    This calculates the checksum of a resource and validates it against the
    checksum stored in its metadata (``premis:hasMessageDigest``).

    :param str uid: UID of the resource to be checked.

    :rtype: None

    :raises: lakesuperior.exceptions.ChecksumValidationError: the cecksums
        do not match. This indicates corruption.
    :raises: lakesuperior.exceptions.IncompatibleLdpTypeError: if the
        resource is not an LDP-NR.
    """
    from lakesuperior.api import resource as rsrc_api
    from lakesuperior.model.ldp.ldp_factory import LDP_NR_TYPE

    rsrc = rsrc_api.get(uid)
    with env.app_globals.rdf_store.txn_ctx():
        if LDP_NR_TYPE not in rsrc.ldp_types:
            raise IncompatibleLdpTypeError()

        ref_digest_term = rsrc.metadata.value(nsc['premis'].hasMessageDigest)
        ref_digest_parts = ref_digest_term.split(':')
        ref_cksum = ref_digest_parts[-1]
        ref_cksum_algo = ref_digest_parts[-2]

        calc_cksum = hashlib.new(ref_cksum_algo, rsrc.content.read()).hexdigest()

    if calc_cksum != ref_cksum:
        raise ChecksumValidationError(uid, ref_cksum, calc_cksum)

    logger.info(f'Fixity check passed for {uid}.')
