from flask import render_template

from lakesuperior.app import create_app
from lakesuperior.config_parser import config


fcrepo = create_app(config['application'], config['logging'])


## GENERIC ROUTES ##

@fcrepo.route('/', methods=['GET'])
def index():
    '''
    Homepage.
    '''
    return render_template('index.html')


@fcrepo.route('/debug', methods=['GET'])
def debug():
    '''
    Debug page.
    '''
    raise RuntimeError()

if __name__ == "__main__":
    fcrepo.run(host='0.0.0.0')
