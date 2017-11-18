import pytest
import uuid

from flask import url_for


@pytest.fixture(scope='module')
def random_uuid():
    return str(uuid.uuid4())


def test_get_root_node(client, db):
    assert client.get(url_for('ldp.get_resource')).status_code == 200


def test_post_resource(client, db):
    '''
    Check response headers for a POST operation with empty payload.
    '''
    res = client.post('/ldp/')
    assert res.status_code == 201
    assert 'Location' in res.headers


def test_put_empty_resource(client, db, random_uuid):
    '''
    Check response headers for a PUT operation with empty payload.
    '''
    res = client.put('/ldp/{}'.format(random_uuid))
    assert res.status_code == 201


def test_put_existing_resource(client, db, random_uuid):
    '''
    Trying to PUT an existing resource should:

    - Return a 204 if the payload is empty
    - Return a 204 if the payload is RDF, server-managed triples are included
      and the 'Prefer' header is set to 'handling=lenient'
    - Return a 409 (ServerManagedTermError) if the payload is RDF,
      server-managed triples are included and handling is set to 'strict'
    '''
    assert client.get('/ldp/{}'.format(random_uuid)).status_code == 200
    assert client.put('/ldp/{}'.format(random_uuid)).status_code == 204
