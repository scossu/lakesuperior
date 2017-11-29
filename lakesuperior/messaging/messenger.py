import logging

from flask import current_app

from lakesuperior.messaging import formatters, handlers

messenger = logging.getLogger('_messaging')


class Messenger:
    '''
    Very simple message sender.
    '''
    _msg_routes = []

    def __init__(self, config):
        for route in config['routes']:
            handler_cls = getattr(handlers, route['handler'])
            messenger.addHandler(handler_cls(route['endpoint']))
            messenger.setLevel(logging.INFO)
            #messenger.formatter = logging.Formatter('%(message)s')
            formatter = getattr(formatters, route['formatter'])

            self._msg_routes.append((messenger, formatter))


    def send(self, *args, **kwargs):
        '''
        Send one or more external messages.
        '''
        for m, f in self._msg_routes:
            m.info(f(*args, **kwargs))
