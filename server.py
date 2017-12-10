from flask import render_template

from lakesuperior.app import create_app
from lakesuperior.config_parser import config


fcrepo = create_app(config['application'], config['logging'])


if __name__ == "__main__":
    fcrepo.run(host='0.0.0.0')
