import multiprocessing
import yaml

from os import chdir, environ, makedirs, getcwd, path

import gunicorn.app.base

from lakesuperior import env, env_setup
from lakesuperior.config_parser import default_config_dir


config_file = path.join(default_config_dir, 'gunicorn.yml')

with open(config_file, 'r') as fh:
    config = yaml.load(fh, yaml.SafeLoader)

listen_addr = config.get('listen_addr', '0.0.0.0')
listen_port = config.get('listen_port', 8000)
preload_app = config.get('preload_app', True)
app_mode = env.app_globals.config['application'].get('app_mode', 'prod')

oldwd = getcwd()
chdir(env.app_globals.config['application']['data_dir'])
data_dir = path.realpath(config.get('data_dir'))
chdir(oldwd)
run_dir = path.join(data_dir, 'run')
log_dir = path.join(data_dir, 'log')
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

    'pidfile': path.join(run_dir, 'fcrepo.pid'),
    'accesslog': path.join(log_dir, 'gunicorn-access.log'),
    'errorlog': path.join(log_dir, 'gunicorn-error.log'),
}
env.wsgi_options = options

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
    from lakesuperior.server import fcrepo
    WsgiApp(fcrepo, options).run()


if __name__ == '__main__':
    run()
