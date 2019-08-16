import logging

from os import path

from flask import Blueprint, jsonify, render_template

from lakesuperior import release
from lakesuperior.dictionaries import srv_mgd_terms as smt

logger = logging.getLogger(__name__)

# Blueprint for main pages. Not much here.

main = Blueprint('main', __name__, template_folder='templates',
        static_folder='templates/static')

## GENERIC ROUTES ##

@main.route('/', methods=['GET'])
def index():
    """Homepage."""
    return render_template('index.html', release=release)


@main.route('/info/ldp_constraints', methods=['GET'])
def ldp_constraints():
    """ LDP term constraints. """
    return jsonify({
        'srv_mgd_subjects': [*smt.srv_mgd_subjects],
        'srv_mgd_predicates': [*smt.srv_mgd_predicates],
        'srv_mgd_types': [*smt.srv_mgd_types],
    })
