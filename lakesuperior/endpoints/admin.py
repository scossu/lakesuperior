import logging

from flask import Blueprint, render_template

from lakesuperior.api import admin as admin_api


# Admin interface and REST API.

logger = logging.getLogger(__name__)
admin = Blueprint('admin', __name__)


@admin.route('/stats', methods=['GET'])
def stats():
    """
    Get repository statistics.
    """
    def fsize_fmt(num, suffix='b'):
        """
        Format an integer into 1024-block file size format.

        Adapted from Python 2 code on
        https://stackoverflow.com/a/1094933/3758232

        :param int num: Size value in bytes.
        :param string suffix: Suffix label (defaults to `B`).

        @return string Formatted size to largest fitting unit.
        """
        for unit in ['','K','M','G','T','P','E','Z']:
            if abs(num) < 1024.0:
                return "{:3.1f} {}{}".format(num, unit, suffix)
            num /= 1024.0
        return "{:.1f} {}{}".format(num, 'Y', suffix)

    repo_stats = admin_api.stats()

    return render_template(
            'stats.html', fsize_fmt=fsize_fmt, **repo_stats)


@admin.route('/tools', methods=['GET'])
def admin_tools():
    """
    Admin tools.

    @TODO stub.
    """
    return render_template('admin_tools.html')
