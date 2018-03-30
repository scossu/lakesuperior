import logging

from lakesuperior.messaging import formatters, handlers

logger = logging.getLogger(__name__)
messenger = logging.getLogger('_messenger')


class Messenger:
    """
    Very simple message sender using the standard Python logging facility.
    """
    def __init__(self, config):
        """
        Set up the messenger.

        :param dict config: Messenger configuration.
        """
        def msg_routes():
            for route in config['routes']:
                handler_cls = getattr(handlers, route['handler'])
                messenger.addHandler(handler_cls(route))
                messenger.setLevel(logging.INFO)
                formatter = getattr(formatters, route['formatter'])

                yield messenger, formatter

        self.config = config
        self.msg_routes = tuple(r for r in msg_routes())
        logger.info('Active messaging routes: {}'.format(self.msg_routes))


    def send(self, *args, **kwargs):
        """Send one or more external messages."""
        for msg, fn in self.msg_routes:
            msg.info(fn(*args, **kwargs))
