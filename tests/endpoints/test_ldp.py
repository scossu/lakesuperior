import pytest
import uuid

from hashlib import sha1

from flask import url_for
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib.term import Literal, URIRef


@pytest.fixture(scope='module')
def random_uuid():
    return str(uuid.uuid4())


def test_get_root_node(client, db):
    #assert client.get(url_for('ldp.get_resource')).status_code == 200
    assert client.get('/ldp').status_code == 200


def test_post_resource(client):
    '''
    Check response headers for a POST operation with empty payload.
    '''
    res = client.post('/ldp/')
    assert res.status_code == 201
    assert 'Location' in res.headers


def test_put_empty_resource(client, random_uuid):
    '''
    Check response headers for a PUT operation with empty payload.
    '''
    res = client.put('/ldp/{}'.format(random_uuid))
    assert res.status_code == 201


def test_put_ldp_rs(client):
    '''
    PUT a resource with RDF payload and verify.
    '''
    with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
        client.put('/ldp/ldprs01', data=f, content_type='text/turtle')

    resp = client.get('/ldp/ldprs01', headers={'accept' : 'text/turtle'})
    assert resp.status_code == 200

    g = Graph().parse(data=resp.data, format='text/turtle')
    assert URIRef('http://vocab.getty.edu/ontology#Subject') in \
            g.objects(None, RDF.type)


def test_put_ldp_nr(client, rnd_img):
    '''
    PUT a resource with binary payload and verify checksums.
    '''
    rnd_img['content'].seek(0)
    client.put('/ldp/ldpnr01', data=rnd_img['content'], headers={
            'Content-Disposition' : 'attachment; filename={}'.format(
            rnd_img['filename'])})

    resp = client.get('/ldp/ldpnr01', headers={'accept' : 'image/png'})
    assert resp.status_code == 200
    assert sha1(resp.data).hexdigest() == rnd_img['hash']


def test_put_existing_resource(client, db, random_uuid):
    '''
    Trying to PUT an existing resource should:

    - Return a 204 if the payload is empty
    - Return a 204 if the payload is RDF, server-managed triples are included
      and the 'Prefer' header is set to 'handling=lenient'
    - Return a 412 (ServerManagedTermError) if the payload is RDF,
      server-managed triples are included and handling is set to 'strict'
    '''
    assert client.get('/ldp/{}'.format(random_uuid)).status_code == 200
    assert client.put('/ldp/{}'.format(random_uuid)).status_code == 204
    with open('tests/data/rdf_payload_w_srv_mgd_trp.ttl', 'rb') as f:
        rsp_len = client.put(
            '/ldp/{}'.format(random_uuid),
            headers={
                'Prefer' : 'handling=lenient',
                'Content-Type' : 'text/turtle',
            },
            data=f
        )
    assert rsp_len.status_code == 204
    with open('tests/data/rdf_payload_w_srv_mgd_trp.ttl', 'rb') as f:
        rsp_strict = client.put(
            '/ldp/{}'.format(random_uuid),
            headers={
                'Prefer' : 'handling=strict',
                'Content-Type' : 'text/turtle',
            },
            data=f
        )
    assert rsp_strict.status_code == 412

