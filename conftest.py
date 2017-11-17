import sys
sys.path.append('.')
import uuid

import pytest

from lakesuperior.app import create_app
from lakesuperior.config_parser import config


@pytest.fixture
def app():
    app = create_app(config['test'], config['logging'])

    return app
