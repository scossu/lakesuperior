import logging

from os import path

from flask import Blueprint, render_template

from lakesuperior import release

logger = logging.getLogger(__name__)

# Blueprint for main pages. Not much here.

main = Blueprint('main', __name__, template_folder='templates',
        static_folder='templates/static')

## GENERIC ROUTES ##

@main.route('/', methods=['GET'])
def index():
    """Homepage."""
    return render_template('index.html', release=release)


@main.route('/debug', methods=['GET'])
def debug():
    """Debug page."""
    raise RuntimeError()


