import logging
import sys

from os import chdir, environ, getcwd, path

import yaml

import lakesuperior

logger = logging.getLogger(__name__)

default_config_dir = environ.get(
        'FCREPO_CONFIG_DIR', path.join(lakesuperior.basedir, 'etc.defaults'))
"""
Default configuration directory.

This value falls back to the provided ``etc.defaults`` directory if the
``FCREPO_CONFIG_DIR`` environment variable is not set.

This value can still be overridden by custom applications by passing the
``config_dir`` value to :func:`parse_config` explicitly.
"""

core_config_dir = path.join(lakesuperior.basedir, 'core_config')


def parse_config(config_dir=None):
    """
    Parse configuration from a directory.

    This is normally called by the standard endpoints (``lsup_admin``, web
    server, etc.) or by a Python client by importing
    :py:mod:`lakesuperior.env.setup()` but an application using a non-default
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

    print(f'Reading configuration at {config_dir}')

    for cname in configs:
        fname = path.join(config_dir, f'{cname}.yml')
        with open(fname, 'r') as fh:
            _config[cname] = yaml.load(fh, yaml.SafeLoader)

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

    logger.info('Graph store location: {}'.format(
        _config['application']['store']['ldp_rs']['location']))
    logger.info('Binary store location: {}'.format(
        _config['application']['store']['ldp_nr']['location']))

    # Merge (and if overlapping, override) custom namespaces with core ones
    with open(path.join(core_config_dir, 'namespaces.yml')) as fh:
        _config['namespaces'].update(yaml.load(fh, yaml.SafeLoader))


    return _config
