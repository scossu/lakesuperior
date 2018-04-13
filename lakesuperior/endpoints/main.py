import logging

from os import path

from flask import Blueprint, render_template

from lakesuperior import basedir

logger = logging.getLogger(__name__)

# Blueprint for main pages. Not much here.

main = Blueprint('main', __name__, template_folder='templates',
        static_folder='templates/static')

## GENERIC ROUTES ##

@main.route('/', methods=['GET'])
def index():
    """Homepage."""
    version_fname = path.abspath(
            path.join(path.dirname(basedir), 'VERSION'))
    with open(version_fname) as fh:
        version = fh.readlines()[0]
    return render_template('index.html', version=version)


@main.route('/debug', methods=['GET'])
def debug():
    """Debug page."""
    raise RuntimeError()


