import sys

from os import path, environ

import hiyapyco
import yaml

import lakesuperior


default_config_dir = environ.get('FCREPO_CONFIG_DIR', path.dirname(
            path.abspath(lakesuperior.__file__)) + '/etc.defaults')
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
        file = '{}/{}.yml'.format(config_dir , cname)
        with open(file, 'r') as stream:
            _config[cname] = yaml.load(stream, yaml.SafeLoader)

    error_msg = '''
    **************
    ** WARNING! **
    **************

    Your test {} store location is set to be the same as the production
    location. This means that if you run a test suite, your live data may be
    wiped clean!

    Please review your configuration before starting.
    '''

    # Merge default and test configurations.
    _test_config = {'application': hiyapyco.load(
            config_dir + '/application.yml',
            config_dir + '/test.yml', method=hiyapyco.METHOD_MERGE)}

    if _config['application']['store']['ldp_rs']['location'] \
            == _test_config['application']['store']['ldp_rs']['location']:
                raise RuntimeError(error_msg.format('RDF'))
                sys.exit()

    if _config['application']['store']['ldp_nr']['path'] \
            == _test_config['application']['store']['ldp_nr']['path']:
                raise RuntimeError(error_msg.format('binary'))
                sys.exit()
    return _config, _test_config


# Load default configuration.
config, test_config = parse_config()
