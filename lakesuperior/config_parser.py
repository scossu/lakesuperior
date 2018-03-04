import os
import sys

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
error_msg = '''
**************
** WARNING! **
**************

Your test {} store endpoint is set to be the same as the production endpoint.
This means that if you run a test suite, your live data may be wiped clean!

Please review your configuration before starting.
'''

test_config = {'application': hiyapyco.load(CONFIG_DIR + '/application.yml',
        CONFIG_DIR + '/test.yml', method=hiyapyco.METHOD_MERGE)}

if config['application']['store']['ldp_rs']['location'] \
        == test_config['application']['store']['ldp_rs']['location']:
            raise RuntimeError(error_msg.format('RDF'))
            sys.exit()

if config['application']['store']['ldp_nr']['path'] \
        == test_config['application']['store']['ldp_nr']['path']:
            raise RuntimeError(error_msg.format('binary'))
            sys.exit()
