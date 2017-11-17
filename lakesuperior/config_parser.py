import os

import hiyapyco
import yaml

configs = (
    'application',
    'logging',
    'namespaces',
    'flask',
)

# This will hold a dict of all configuration values.
config = {}

# Parse configuration
if 'FCREPO_CONFIG_DIR' in os.environ:
    CONFIG_DIR = os.environ['FCREPO_CONFIG_DIR']
else:
    CONFIG_DIR = os.path.dirname(os.path.abspath(__file__)) + '/etc'

for cname in configs:
    file = '{}/{}.yml'.format(CONFIG_DIR , cname)
    with open(file, 'r') as stream:
        config[cname] = yaml.load(stream, yaml.SafeLoader)

# Merge default and test configurations.
config['test'] = hiyapyco.load(CONFIG_DIR + '/application.yml',
        CONFIG_DIR + '/test.yml', method=hiyapyco.METHOD_MERGE)
