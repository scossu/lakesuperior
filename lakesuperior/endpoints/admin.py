import logging

from flask import Blueprint, render_template

from lakesuperior.env import env
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager

# Admin interface and API.

app_globals = env.app_globals
logger = logging.getLogger(__name__)

admin = Blueprint('admin', __name__)


@admin.route('/stats', methods=['GET'])
def stats():
    '''
    Get repository statistics.
    '''
    def fsize_fmt(num, suffix='B'):
        '''
        Format an integer into 1024-block file size format.

        Adapted from Python 2 code on
        https://stackoverflow.com/a/1094933/3758232

        @param num (int) Size value in bytes.
        @param suffix (string) Suffix label (defaults to `B`).

        @return string Formatted size to largest fitting unit.
        '''
        for unit in ['','K','M','G','T','P','E','Z']:
            if abs(num) < 1024.0:
                return "{:3.1f} {}{}".format(num, unit, suffix)
            num /= 1024.0
        return "{:.1f} {}{}".format(num, 'Y', suffix)

    with TxnManager(app_globals.rdf_store) as txn:
        store_stats = app_globals.rdf_store.stats()
    rsrc_stats = app_globals.rdfly.count_rsrc()
    return render_template(
            'stats.html', rsrc_stats=rsrc_stats, store_stats=store_stats,
            fsize_fmt=fsize_fmt)


@admin.route('/tools', methods=['GET'])
def admin_tools():
    '''
    Admin tools.

    @TODO stub.
    '''
    return render_template('admin_tools.html')
