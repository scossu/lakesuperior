import sys

from os import chdir, environ, getcwd, path

import hiyapyco
import yaml

import lakesuperior


default_config_dir = environ.get(
        'FCREPO_CONFIG_DIR',
        path.join(
            path.dirname(path.abspath(lakesuperior.__file__)), 'etc.defaults'))
"""
Default configuration directory.

This value falls back to the provided ``etc.defaults`` directory if the
``FCREPO_CONFIG_DIR`` environment variable is not set.

This value can still be overridden by custom applications by passing the
``config_dir`` value to :func:`parse_config` explicitly.
"""


def parse_config(config_dir=None):
    """
    Parse configuration from a directory.

    This is normally called by the standard endpoints (``lsup_admin``, web
    server, etc.) or by a Python client by importing
    :py:mod:`lakesuperior.env_setup` but an application using a non-default
    configuration may specify an alternative configuration directory.

    The directory must have the same structure as the one provided in
    ``etc.defaults``.

    :param config_dir: Location on the filesystem of the configuration
        directory. The default is set by the ``FCREPO_CONFIG_DIR`` environment
        variable or, if this is not set, the ``etc.defaults`` stock directory.
    """
    configs = (
        'application',
        'logging',
        'namespaces',
        'flask',
    )

    if not config_dir:
        config_dir = default_config_dir

    # This will hold a dict of all configuration values.
    _config = {}

    print('Reading configuration at {}'.format(config_dir))

    for cname in configs:
        file = path.join(config_dir, '{}.yml'.format(cname))
        with open(file, 'r') as stream:
            _config[cname] = yaml.load(stream, yaml.SafeLoader)

    if not _config['application']['data_dir']:
        _config['application']['data_dir'] = path.join(
                lakesuperior.basedir, 'data')

    data_dir = _config['application']['data_dir']
    _config['application']['store']['ldp_nr']['location'] = path.join(
            data_dir, 'ldpnr_store')
    _config['application']['store']['ldp_rs']['location'] = path.join(
            data_dir, 'ldprs_store')
    # If log handler file names are relative, they will be relative to the
    # data dir.
    oldwd = getcwd()
    chdir(data_dir)
    for handler in _config['logging']['handlers'].values():
        if 'filename' in handler:
            handler['filename'] = path.realpath(handler['filename'])
    chdir(oldwd)

    return _config


# Load default configuration.
config = parse_config()
