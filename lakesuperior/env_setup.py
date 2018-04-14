from lakesuperior import env
from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals

__doc__="""
Default configuration.

Import this module to initialize the configuration for a production setup::

    >>> import lakesuperior.env_setup

Will load the default configuration.
"""

env.app_globals = AppGlobals(config)
