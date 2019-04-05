import multiprocessing
import os
import yaml

# DO NOT load env_setup here. It must be loaded after workers have been forked.
from lakesuperior.config_parser import (
        config as main_config, default_config_dir)


__doc__ = """
GUnicorn WSGI configuration.

GUnicorn reads configuration options from this file by importing it::

    gunicorn -c python:lakesuperior.wsgi lakesuperior.server:fcrepo

This module reads the ``gunicorn.yml`` configuration and overrides defaults
set here. Only some of the GUnicorn optionscan be changed: others have to be
set to specific values in order for Lakesuperior to work properly.
"""

__all__ = [
    'bind',
    'workers',
    'worker_class',
    'max_requests',
    'user',
    'group',
    'raw_env',
    'preload_app',
    'daemon',
    'reload',
    'pidfile',
    'accesslog',
    'errorlog',
]
"""
Variables to export to GUnicorn.

Not sure if this does anything—GUnicorn doesn't seem to use ``import *``—but
at least good for maintainers of this code.
"""


class __Defaults:
    """
    Gather default values for WSGI config.
    """
    config_file = os.path.join(default_config_dir, 'gunicorn.yml')

    listen_addr = '0.0.0.0'
    listen_port = 8000
    preload_app = False
    app_mode = 'prod'
    worker_class = 'sync'
    max_requests = 0

    def __init__(self):
        with open(self.config_file, 'r') as fh:
            self.config = yaml.load(fh, yaml.SafeLoader)

        oldwd = os.getcwd()
        os.chdir(main_config['application']['data_dir'])
        # Set data directory relatively to startup script.
        self.data_dir = os.path.realpath(self.config.get('data_dir'))
        os.chdir(oldwd)
        self.run_dir = os.path.join(self.data_dir, 'run')
        self.log_dir = os.path.join(self.data_dir, 'log')
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.run_dir, exist_ok=True)
        self.workers = (multiprocessing.cpu_count() * 2) + 1


__def = __Defaults()
__app_mode = main_config['application'].get('app_mode', __def.app_mode)

# Exposed Gunicorn parameters begin here.

bind = '{}:{}'.format(
        __def.config.get('listen_addr', __def.listen_addr),
        __def.config.get('listen_port', __def.listen_port))
workers = __def.config.get('workers', __def.workers)
worker_class = __def.config.get('worker_class', __def.worker_class)
max_requests = __def.config.get('max_requests', __def.max_requests)

user = __def.config.get('user')
group = __def.config.get('group')

raw_env = 'APP_MODE={}'.format(__app_mode)

preload_app = __def.config.get('preload_app', __def.preload_app)
daemon = __app_mode == 'prod'
reload = __app_mode == 'dev' and not preload_app

pidfile = os.path.join(__def.run_dir, 'fcrepo.pid')
accesslog = os.path.join(__def.log_dir, 'gunicorn-access.log')
errorlog = os.path.join(__def.log_dir, 'gunicorn-error.log')

print('\nLoading WSGI server with configuration:')
for prop in __all__:
    print(f'{prop:>16} = {locals().get(prop)}')
print('\n')
