import threading

from os import path

basedir = path.dirname(path.realpath(__file__))
"""
Base directory for the module.

This can be used by modules looking for configuration and data files to be
referenced or copied with a known path relative to the package root.

:rtype: str
"""

class Env:
    pass

env = Env()
"""
A pox on "globals are evil".

All-purpose bucket for storing global variables. Different environments
(e.g. webapp, test suite) put the appropriate value in it.
The most important values to be stored are app_conf (either from
lakesuperior.config_parser.config or lakesuperior.config_parser.test_config)
and app_globals (obtained by an instance of lakesuperior.globals.AppGlobals).

e.g.::

    >>> from lakesuperior.config_parser import config
    >>> from lakesuperior.globals import AppGlobals
    >>> from lakesuperior import env
    >>> env.app_globals = AppGlobals(config)

This is automated in non-test environments by importing
`lakesuperior.env_setup`.

:rtype: Object
"""

thread_env = threading.local()
"""
Thread-local environment.

This is used to store thread-specific variables such as start/end request
timestamps.

:rtype: threading.local
"""
