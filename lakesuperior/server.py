import logging

from logging.config import dictConfig

from lakesuperior import env
# Environment must be set before importing the app factory function.
env.setup()

from lakesuperior.app import create_app

dictConfig(env.app_globals.config['logging'])
logger = logging.getLogger(__name__)

# this stays at the module level so it's used by GUnicorn.
fcrepo = create_app(env.app_globals.config['application'])

def run():
    fcrepo.run(host='0.0.0.0')

if __name__ == "__main__":
    run()
