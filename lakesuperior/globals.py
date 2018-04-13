import logging

from collections import deque
from importlib import import_module

from lakesuperior.dictionaries.namespaces import ns_collection as nsc

RES_CREATED = '_create_'
"""A resource was created."""
RES_DELETED = '_delete_'
"""A resource was deleted."""
RES_UPDATED = '_update_'
"""A resource was updated."""

ROOT_UID = '/'
"""Root node UID."""
ROOT_RSRC_URI = nsc['fcres'][ROOT_UID]
"""Internal URI of root resource."""


class AppGlobals:
    """
    Application Globals.

    This class is instantiated and used as a carrier for all connections and
    various global variables outside of the Flask app context.

    The variables are set on initialization by passing a configuration dict.
    Usually this is done when starting an application. The instance with the
    loaded variables is then assigned to the :data:`lakesuperior.env`
    global variable.

    You can either load the default configuration::

        >>>from lakesuperior import env_setup

    Or set up an environment with a custom configuration::

        >>> from lakesuperior import env
        >>> from lakesuperior.app_globals import AppGlobals
        >>> my_config = {'name': 'value', '...': '...'}
        >>> env.app_globals = AppGlobals(my_config)

    """
    def __init__(self, config):
        """
        Generate global variables from configuration.
        """
        from lakesuperior.messaging.messenger import Messenger

        app_conf = config['application']

        # Initialize RDF layout.
        rdfly_mod_name = app_conf['store']['ldp_rs']['layout']
        rdfly_mod = import_module('lakesuperior.store.ldp_rs.{}'.format(
                rdfly_mod_name))
        rdfly_cls = getattr(rdfly_mod, self.camelcase(rdfly_mod_name))
        #logger.info('RDF layout: {}'.format(rdfly_mod_name))

        # Initialize file layout.
        nonrdfly_mod_name = app_conf['store']['ldp_nr']['layout']
        nonrdfly_mod = import_module('lakesuperior.store.ldp_nr.{}'.format(
                nonrdfly_mod_name))
        nonrdfly_cls = getattr(nonrdfly_mod, self.camelcase(nonrdfly_mod_name))
        #logger.info('Non-RDF layout: {}'.format(nonrdfly_mod_name))

        # Set up messaging.
        self._messenger  = Messenger(app_conf['messaging'])

        # Exposed globals.
        self._config = config
        self._rdfly = rdfly_cls(app_conf['store']['ldp_rs'])
        self._nonrdfly = nonrdfly_cls(app_conf['store']['ldp_nr'])
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

        This is an instance of
        :class:`~lakesuperior.store.ldp_rs.rsrc_centric_layout.RsrcCentricLayout`.

        *TODO:* Update class reference when interface will be separated from
        implementation.
        """
        return self._rdfly

    @property
    def rdf_store(self):
        """
        Current RDF low-level store.

        This is an instance of
        :class:`~lakesuperior.store.ldp_rs.lmdb_store.LmdbStore`.
        """
        return self._rdfly.store

    @property
    def nonrdfly(self):
        return self._nonrdfly
        """
        Current non-RDF (binary contents) layout.

        This is an instance of
        :class:`~lakesuperior.store.ldp_nr.base_non_rdf_layout.BaseNonRdfLayout`.
        """

    @property
    def messenger(self):
        """
        Current message handler.

        This is an instance of
        :class:`~lakesuperior.messaging.messenger.Messenger`.
        """
        return self._messenger

    @property
    def changelog(self):
        return self._changelog


    def camelcase(self, word):
        """
        Convert a string with underscores to a camel-cased one.

        Ripped from https://stackoverflow.com/a/6425628
        """
        return ''.join(x.capitalize() or '_' for x in word.split('_'))
