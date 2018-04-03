import multiprocessing
import yaml

from os import environ, makedirs, path

import gunicorn.app.base

from lakesuperior.server import fcrepo


default_config_dir = '{}/etc.defaults'.format(
        path.dirname(path.abspath(__file__)))
config_dir = environ.get('FCREPO_CONFIG_DIR', default_config_dir)
config_file = '{}/gunicorn.yml'.format(config_dir)

with open(config_file, 'r') as fh:
    config = yaml.load(fh, yaml.SafeLoader)

listen_addr = config.get('listen_addr', '0.0.0.0')
listen_port = config.get('listen_port', 8000)
preload_app = config.get('preload_app', True)
app_mode = config.get('app_mode', 'prod')

data_dir = path.realpath(config.get('data_dir'))
run_dir = '{}/run'.format(data_dir)
log_dir = '{}/log'.format(data_dir)
makedirs(log_dir, exist_ok=True)
makedirs(run_dir, exist_ok=True)

def default_workers():
    return (multiprocessing.cpu_count() * 2) + 1

options = {
    'bind': '{}:{}'.format(listen_addr, listen_port),
    'workers': config.get('workers', default_workers()),
    'worker_class': config.get('worker_class', 'gevent'),
    'max_requests': config.get('max_requests', 512),

    'user': config.get('user'),
    'group': config.get('group'),

    'raw_env': 'APP_MODE={}'.format(app_mode),

    'preload_app': preload_app,
    'daemon': app_mode=='prod',
    'reload': app_mode=='dev' and not preload_app,

    'pidfile': '{}/fcrepo.pid'.format(run_dir),
    'accesslog': '{}/gunicorn-access.log'.format(log_dir),
    'errorlog': '{}/gunicorn-error.log'.format(log_dir),
}

class WsgiApp(gunicorn.app.base.BaseApplication):

    def __init__(self, app, options={}):
        self.options = options
        self.application = app
        super(WsgiApp, self).__init__()

    def load_config(self):
        for key, value in self.options.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


def run():
    WsgiApp(fcrepo, options).run()


if __name__ == '__main__':
    run()
