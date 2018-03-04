from lakesuperior.app import create_app
from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals
from lakesuperior.env import env

env.config = config
env.app_globals = AppGlobals(config)
dictConfig(env.config['logging'])

fcrepo = create_app(env.config['application'])

if __name__ == "__main__":
    fcrepo.run(host='0.0.0.0')
