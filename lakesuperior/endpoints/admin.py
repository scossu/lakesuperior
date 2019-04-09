import logging

from flask import Blueprint, jsonify, render_template

from lakesuperior.api import admin as admin_api
from lakesuperior.exceptions import (
    ChecksumValidationError, ResourceNotExistsError, TombstoneError)
from lakesuperior.util.toolbox import fsize_fmt


# Admin interface and REST API.

logger = logging.getLogger(__name__)
admin = Blueprint('admin', __name__)


@admin.route('/stats', methods=['GET'])
def stats():
    """
    Get repository statistics.
    """
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


@admin.route('/<path:uid>/fixity', methods=['GET'])
def fixity_check(uid):
    """
    Check the fixity of a resource.
    """
    uid = '/' + uid.strip('/')

    try:
        admin_api.fixity_check(uid)
    except ResourceNotExistsError as e:
        return str(e), 404
    except TombstoneError as e:
        return str(e), 410
    except ChecksumValidationError as e:
        check_pass = False
    else:
        check_pass = True


    return (
        jsonify({
            'uid': uid,
            'pass': check_pass,
        }),
        200,
        {'content-type': 'application/json'}
    )
