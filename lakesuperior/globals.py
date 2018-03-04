import logging

from collections import deque
from importlib import import_module

from lakesuperior.dictionaries.namespaces import ns_collection as nsc

'''
Constants used in messaging to identify an event type.
'''
RES_CREATED = '_create_'
RES_DELETED = '_delete_'
RES_UPDATED = '_update_'

ROOT_UID = ''
ROOT_RSRC_URI = nsc['fcres'][ROOT_UID]


class AppGlobals:
    '''
    Application Globals.

    This class sets up all connections and exposes them across the application
    outside of the Flask app context.
    '''
    def __init__(self, conf):
        from lakesuperior.messaging.messenger import Messenger

        app_conf = conf['application']

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
        messenger = Messenger(app_conf['messaging'])

        # Exposed globals.
        self._rdfly = rdfly_cls(app_conf['store']['ldp_rs'])
        self._nonrdfly = nonrdfly_cls(app_conf['store']['ldp_nr'])
        self._messenger = messenger
        self._changelog = deque()


    @property
    def rdfly(self):
        return self._rdfly

    @property
    def rdf_store(self):
        return self._rdfly.store

    @property
    def nonrdfly(self):
        return self._nonrdfly

    @property
    def messenger(self):
        return self._messenger

    @property
    def changelog(self):
        return self._changelog


    def camelcase(self, word):
        '''
        Convert a string with underscores to a camel-cased one.

        Ripped from https://stackoverflow.com/a/6425628
        '''
        return ''.join(x.capitalize() or '_' for x in word.split('_'))
