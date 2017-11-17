#!/usr/bin/env python

from lakesuperior.config_parser import config

# This script will parse configuration files and initialize a filesystem and
# triplestore with an empty FCREPO repository.
#
# Additional, scaffolding files may be parsed to create initial contents.

# @TODO

# Initialize temporary folders.
tmp_path = config['application']['store']['ldp_nr']['path'] + '/tmp'
if not os.path.exists(tmp_path):
    os.makedirs(tmp_path)

