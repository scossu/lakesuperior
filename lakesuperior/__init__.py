import logging
import threading

from collections import deque
from importlib import import_module
from os import path


logger = logging.getLogger(__name__)

version = '1.0 alpha'
release = '1.0.0a22'

basedir = path.dirname(path.realpath(__file__))
"""
Base directory for the module.

This can be used by modules looking for configuration and data files to be
referenced or copied with a known path relative to the package root.

:rtype: str
"""

class Env:
    """
    Lakesuperior environment.

    Instances of this class contain the environment necessary to run a
    self-standing instance of Lakesuperior in a Python environment.
    """

    def setup(self, config_dir=None, config=None):
        """
        Set the environment up.

        This must be done before using the application.

        This method will warn and not do anything if it has already been
        called in the same runtime environment,

        :param str config_dir: Path to a directory containing the
            configuration ``.yml`` files. If this and ``config`` are omitted,
            the configuration files are read from the default directory defined
            in :py:meth:`~lakesuperior.config_parser.parse_config()`.

        :param dict config: Fully-formed configuration as a dictionary. If
            this is provided, ``config_dir`` is ignored. This is useful to
            call ``parse_config()`` separately and modify the configuration
            manually before passing it to the setup.
        """
        if hasattr(self, 'app_globals'):
            logger.warn('The environment is already set up.')
            return

        if not config:
            from .config_parser import parse_config
            config = parse_config(config_dir)

        self.app_globals = _AppGlobals(config)


env = Env()
"""
A pox on "globals are evil".

Object for storing global variables. Different environments
(e.g. webapp, test suite) put the appropriate value in it.
The most important values to be stored are app_conf (either from
lakesuperior.config_parser.config or lakesuperior.config_parser.test_config)
and app_globals (obtained by an instance of lakesuperior.globals.AppGlobals).

e.g.::

    >>> from lakesuperior import env
    >>> env.setup()

Or, with a custom configuration directory::

    >>> from lakesuperior import env
    >>> env.setup('/my/config/dir')

Or, to load a configuration and modify it before setting up the environment::

    >>> from lakesuperior import env
    >>> from lakesuperior.config_parser import parse_config
    >>> config = parse_config(config_dir)
    >>> config['application']['data_dir'] = '/data/ext/mystore'
    >>> env.setup(config=config)

:rtype: Object
"""

thread_env = threading.local()
"""
Thread-local environment.

This is used to store thread-specific variables such as start/end request
timestamps.

:rtype: threading.local
"""


## Private members. Nothing interesting here.

class _AppGlobals:
    """
    Application Globals.

    This class is instantiated and used as a carrier for all connections and
    various global variables outside of the Flask app context.

    The variables are set on initialization by passing a configuration dict.
    Usually this is done when starting an application. The instance with the
    loaded variables is then assigned to the :data:`lakesuperior.env`
    global variable.

    :see_also: lakesuperior.env.setup()

    """
    def __init__(self, config):
        """
        Generate global variables from configuration.
        """
        ## Initialize metadata store.
        #from lakesuperior.store.metadata_store import MetadataStore

        # Exposed globals.
        self._config = config
        #self._md_store = MetadataStore(path.join(
        #        self.config['application']['data_dir'], 'metadata'),
        #        create=True)
        self._changelog = deque()


    @property
    def config(self):
        """
        Global configuration.

        This is a collection of all configuration options **except** for the
        WSGI configuration which is initialized at a different time and is
        stored under :data:`lakesuperior.env.wsgi_options`.

        *TODO:* Update class reference when interface will be separated from
        implementation.
        """
        return self._config

    @property
    def rdfly(self):
        """
        Current RDF layout.

        Lazy loaded because it needs the config to be set up.

        This is an instance of
        :class:`~lakesuperior.store.ldp_rs.rsrc_centric_layout.RsrcCentricLayout`.

        *TODO:* Update class reference when interface will be separated from
        implementation.
        """
        if not hasattr(self, '_rdfly'):
            # Initialize RDF layout.
            rdfly_mod_name = (
                    self.config['application']['store']['ldp_rs']['layout'])
            rdfly_mod = import_module('lakesuperior.store.ldp_rs.{}'.format(
                    rdfly_mod_name))
            rdfly_cls = getattr(rdfly_mod, self.camelcase(rdfly_mod_name))
            #logger.info('RDF layout: {}'.format(rdfly_mod_name))
            self._rdfly = rdfly_cls(
                    self.config['application']['store']['ldp_rs'])

        return self._rdfly

    @property
    def rdf_store(self):
        """
        Current RDF low-level store.

        Lazy loaded because it needs the config to be set up.

        This is an instance of
        :class:`~lakesuperior.store.ldp_rs.lmdb_store.LmdbStore`.
        """
        return self.rdfly.store

    @property
    def nonrdfly(self):
        """
        Current non-RDF (binary contents) layout.

        Lazy loaded because it needs the config to be set up.

        This is an instance of
        :class:`~lakesuperior.store.ldp_nr.base_non_rdf_layout.BaseNonRdfLayout`.
        """
        if not hasattr(self, '_nonrdfly'):
            # Initialize file layout.
            nonrdfly_mod_name = (
                    self.config['application']['store']['ldp_nr']['layout'])
            nonrdfly_mod = import_module('lakesuperior.store.ldp_nr.{}'.format(
                    nonrdfly_mod_name))
            nonrdfly_cls = getattr(nonrdfly_mod, self.camelcase(nonrdfly_mod_name))
            #logger.info('Non-RDF layout: {}'.format(nonrdfly_mod_name))
            self._nonrdfly = nonrdfly_cls(
                    self.config['application']['store']['ldp_nr'])
        return self._nonrdfly

    #@property
    #def md_store(self):
    #    """
    #    Metadata store (LMDB).

    #    This is an instance of
    #    :class:`~lakesuperior.store.metadata_store.MetadataStore`.
    #    """
    #    return self._md_store

    @property
    def messenger(self):
        """
        Current message handler.

        Lazy loaded because it needs the config to be set up.

        This is an instance of
        :class:`~lakesuperior.messaging.messenger.Messenger`.
        """
        if not hasattr(self, '_messenger'):
            from lakesuperior.messaging.messenger import Messenger
            self._messenger  = Messenger(
                    self.config['application']['messaging'])

        return self._messenger

    @property
    def changelog(self):
        return self._changelog


    @staticmethod
    def camelcase(word):
        """
        Convert a string with underscores to a camel-cased one.

        Ripped from https://stackoverflow.com/a/6425628
        """
        return ''.join(x.capitalize() or '_' for x in word.split('_'))
