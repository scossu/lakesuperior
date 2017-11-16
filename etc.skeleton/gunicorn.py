# See: http://docs.gunicorn.org/en/stable/settings.html

# Directory where to store logs, PIDfile, etc.
_data_dir = '/tmp'

# Set app_mode to either 'prod', 'test' or 'dev'.
# 'prod' is normal running mode. 'test' is used for running test suites.
# 'dev' is similar to normal mode but with reload and debug enabled.
_app_mode = 'dev'


bind = "0.0.0.0:8000"
workers = 4
threads = 2
max_requests = 1000

#user = "user"
#group = "group"

raw_env = 'APP_MODE={}'.format(_app_mode)

# Set this to the directory containing logs, etc.
# The path must end with a slash.
#chdir = "/usr/local/lakesuperior/"

daemon = _app_mode=='prod'
pidfile = _data_dir + "run/fcrepo.pid"
reload = _app_mode=='dev'

accesslog = _data_dir + "log/gunicorn-access.log"
errorlog = _data_dir + "log/gunicorn-error.log"

