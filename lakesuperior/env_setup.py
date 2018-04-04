from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals
from lakesuperior.env import env

__doc__="""
Default configuration.

Import this module to initialize the configuration for a production setup::

    >>>from lakesuperior import env_setup

Will load the default configuration.
"""

env.config = config
env.app_globals = AppGlobals(config)
