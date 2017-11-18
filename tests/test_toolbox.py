import pytest

from lakesuperior.toolbox import Toolbox

def test_camelcase(client):
    c = client.get('/ldp')
    in_str = 'test_input_string'
    assert Toolbox().camelcase(in_str) == 'TestInputString'
