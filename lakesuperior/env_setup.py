from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals
from lakesuperior.env import env

'''
Import this module to initialize the configuration for a production setup.
'''
env.config = config
env.app_globals = AppGlobals(config)
