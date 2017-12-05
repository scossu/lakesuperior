import pytest
import uuid

from hashlib import sha1

from flask import url_for
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib.term import Literal, URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.toolbox import Toolbox

@pytest.fixture(scope='module')
def random_uuid():
    return str(uuid.uuid4())


@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestLdp:
    '''
    Test HTTP interaction with LDP endpoint.
    '''
    def test_get_root_node(self):
        '''
        Get the root node from two different endpoints.

        The test triplestore must be initialized, hence the `db` fixture.
        '''
        ldp_resp = self.client.get('/ldp')
        rest_resp = self.client.get('/rest')

        assert ldp_resp.status_code == 200
        assert rest_resp.status_code == 200
        #assert ldp_resp.data == rest_resp.data


    def test_put_empty_resource(self, random_uuid):
        '''
        Check response headers for a PUT operation with empty payload.
        '''
        res = self.client.put('/ldp/{}'.format(random_uuid))
        assert res.status_code == 201


    def test_put_existing_resource(self, random_uuid):
        '''
        Trying to PUT an existing resource should return a 204 if the payload
        is empty.
        '''
        path = '/ldp/nonidempotent01'
        assert self.client.put(path).status_code == 201
        assert self.client.get(path).status_code == 200
        assert self.client.put(path).status_code == 204


    def test_put_ldp_rs(self, client):
        '''
        PUT a resource with RDF payload and verify.
        '''
        with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
            self.client.put('/ldp/ldprs01', data=f, content_type='text/turtle')

        resp = self.client.get('/ldp/ldprs01',
                headers={'accept' : 'text/turtle'})
        assert resp.status_code == 200

        g = Graph().parse(data=resp.data, format='text/turtle')
        assert URIRef('http://vocab.getty.edu/ontology#Subject') in \
                g.objects(None, RDF.type)


    def test_put_ldp_nr(self, rnd_img):
        '''
        PUT a resource with binary payload and verify checksums.
        '''
        rnd_img['content'].seek(0)
        resp = self.client.put('/ldp/ldpnr01', data=rnd_img['content'],
                headers={
                    'Content-Disposition' : 'attachment; filename={}'.format(
                    rnd_img['filename'])})
        assert resp.status_code == 201

        resp = self.client.get('/ldp/ldpnr01', headers={'accept' : 'image/png'})
        assert resp.status_code == 200
        assert sha1(resp.data).hexdigest() == rnd_img['hash']


    def test_post_resource(self, client):
        '''
        Check response headers for a POST operation with empty payload.
        '''
        res = self.client.post('/ldp/')
        assert res.status_code == 201
        assert 'Location' in res.headers


    def test_post_slug(self):
        '''
        Verify that a POST with slug results in the expected URI only if the
        resource does not exist already.
        '''
        slug01_resp = self.client.post('/ldp', headers={'slug' : 'slug01'})
        assert slug01_resp.status_code == 201
        assert slug01_resp.headers['location'] == \
                Toolbox().base_url + '/slug01'

        slug02_resp = self.client.post('/ldp', headers={'slug' : 'slug01'})
        assert slug02_resp.status_code == 201
        assert slug02_resp.headers['location'] != \
                Toolbox().base_url + '/slug01'


    def test_post_404(self):
        '''
        Verify that a POST to a non-existing parent results in a 404.
        '''
        assert self.client.post('/ldp/{}'.format(uuid.uuid4()))\
                .status_code == 404


    def test_post_409(self, rnd_img):
        '''
        Verify that you cannot POST to a binary resource.
        '''
        rnd_img['content'].seek(0)
        self.client.put('/ldp/post_409', data=rnd_img['content'], headers={
                'Content-Disposition' : 'attachment; filename={}'.format(
                rnd_img['filename'])})
        assert self.client.post('/ldp/post_409').status_code == 409


    def test_patch(self):
        '''
        Test patching a resource.
        '''
        path = '/ldp/test_patch01'
        self.client.put(path)

        uri = Toolbox().base_url + '/test_patch01'

        self.client.patch(path,
                data=open('tests/data/sparql_update/simple_insert.sparql'),
                headers={'content-type' : 'application/sparql-update'})

        resp = self.client.get(path)
        g = Graph().parse(data=resp.data, format='text/turtle')
        print('Triples after first PATCH: {}'.format(set(g)))
        assert g[ URIRef(uri) : nsc['dc'].title : Literal('Hello') ]

        self.client.patch(path,
                data=open('tests/data/sparql_update/delete+insert+where.sparql'),
                headers={'content-type' : 'application/sparql-update'})

        resp = self.client.get(path)
        g = Graph().parse(data=resp.data, format='text/turtle')
        assert g[ URIRef(uri) : nsc['dc'].title : Literal('Ciao') ]


    def test_delete(self):
        '''
        Test delete response codes.
        '''
        create_resp = self.client.put('/ldp/test_delete01')
        delete_resp = self.client.delete('/ldp/test_delete01')
        assert delete_resp.status_code == 204

        bogus_delete_resp = self.client.delete('/ldp/test_delete101')
        assert bogus_delete_resp.status_code == 404


    def test_tombstone(self):
        '''
        Test tombstone behaviors.
        '''
        tstone_resp = self.client.get('/ldp/test_delete01')
        assert tstone_resp.status_code == 410
        assert tstone_resp.headers['Link'] == \
                '<{}/test_delete01/fcr:tombstone>; rel="hasTombstone"'\
                .format(Toolbox().base_url)

        tstone_path = '/ldp/test_delete01/fcr:tombstone'
        assert self.client.get(tstone_path).status_code == 405
        assert self.client.put(tstone_path).status_code == 405
        assert self.client.post(tstone_path).status_code == 405
        assert self.client.delete(tstone_path).status_code == 204

        assert self.client.get('/ldp/test_delete01').status_code == 404


    def test_delete_recursive(self):
        '''
        Test response codes for resources deleted recursively and their
        tombstones.
        '''
        self.client.put('/ldp/test_delete_recursive01')
        self.client.put('/ldp/test_delete_recursive01/a')

        self.client.delete('/ldp/test_delete_recursive01')

        tstone_resp = self.client.get('/ldp/test_delete_recursive01')
        assert tstone_resp.status_code == 410
        assert tstone_resp.headers['Link'] == \
            '<{}/test_delete_recursive01/fcr:tombstone>; rel="hasTombstone"'\
            .format(Toolbox().base_url)

        child_tstone_resp = self.client.get('/ldp/test_delete_recursive01/a')
        assert child_tstone_resp.status_code == tstone_resp.status_code
        assert 'Link' not in child_tstone_resp.headers.keys()



@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestPrefHeader:
    '''
    Test various combinations of `Prefer` header.
    '''
    @pytest.fixture(scope='class')
    def cont_structure(self):
        '''
        Create a container structure to be used for subsequent requests.
        '''
        parent_path = '/ldp/test_parent'
        self.client.put(parent_path)
        self.client.put(parent_path + '/child1')
        self.client.put(parent_path + '/child2')
        self.client.put(parent_path + '/child3')

        return {
            'path' : parent_path,
            'response' : self.client.get(parent_path),
            'subject' : URIRef(Toolbox().base_url + '/test_parent')
        }


    def test_put_prefer_handling(self, random_uuid):
        '''
        Trying to PUT an existing resource should:

        - Return a 204 if the payload is empty
        - Return a 204 if the payload is RDF, server-managed triples are
          included and the 'Prefer' header is set to 'handling=lenient'
        - Return a 412 (ServerManagedTermError) if the payload is RDF,
          server-managed triples are included and handling is set to 'strict'
        '''
        path = '/ldp/put_pref_header01'
        assert self.client.put(path).status_code == 201
        assert self.client.get(path).status_code == 200
        assert self.client.put(path).status_code == 204
        with open('tests/data/rdf_payload_w_srv_mgd_trp.ttl', 'rb') as f:
            rsp_len = self.client.put(
                path,
                headers={
                    'Prefer' : 'handling=lenient',
                    'Content-Type' : 'text/turtle',
                },
                data=f
            )
        assert rsp_len.status_code == 204
        with open('tests/data/rdf_payload_w_srv_mgd_trp.ttl', 'rb') as f:
            rsp_strict = self.client.put(
                path,
                headers={
                    'Prefer' : 'handling=strict',
                    'Content-Type' : 'text/turtle',
                },
                data=f
            )
        assert rsp_strict.status_code == 412


    def test_embed_children(self, cont_structure):
        '''
        verify the "embed children" prefer header.
        '''
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = cont_structure['subject']

        minimal_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=minimal',
        })

        incl_embed_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.EMBED_CHILD_RES_URI),
        })
        omit_embed_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.EMBED_CHILD_RES_URI),
        })

        assert omit_embed_children_resp.data == cont_resp.data

        incl_g = Graph().parse(
                data=incl_embed_children_resp.data, format='turtle')
        omit_g = Graph().parse(
                data=omit_embed_children_resp.data, format='turtle')

        children = set(incl_g[cont_subject : nsc['ldp'].contains])
        assert len(children) == 3

        children = set(incl_g[cont_subject : nsc['ldp'].contains])
        for child_uri in children:
            assert set(incl_g[ child_uri : : ])
            assert not set(omit_g[ child_uri : : ])


    def test_return_children(self, cont_structure):
        '''
        verify the "return children" prefer header.
        '''
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = cont_structure['subject']

        incl_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.RETURN_CHILD_RES_URI),
        })
        omit_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.RETURN_CHILD_RES_URI),
        })

        assert incl_children_resp.data == cont_resp.data

        incl_g = Graph().parse(data=incl_children_resp.data, format='turtle')
        omit_g = Graph().parse(data=omit_children_resp.data, format='turtle')

        children = incl_g[cont_subject : nsc['ldp'].contains]
        for child_uri in children:
            assert not omit_g[ cont_subject : nsc['ldp'].contains : child_uri ]


    def test_inbound_rel(self, cont_structure):
        '''
        verify the "inboud relationships" prefer header.
        '''
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = cont_structure['subject']

        incl_inbound_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.RETURN_INBOUND_REF_URI),
        })
        omit_inbound_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.RETURN_INBOUND_REF_URI),
        })

        assert omit_inbound_resp.data == cont_resp.data

        incl_g = Graph().parse(data=incl_inbound_resp.data, format='turtle')
        omit_g = Graph().parse(data=omit_inbound_resp.data, format='turtle')

        assert set(incl_g[ : : cont_subject ])
        assert not set(omit_g[ : : cont_subject ])


    def test_srv_mgd_triples(self, cont_structure):
        '''
        verify the "server managed triples" prefer header.
        '''
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = cont_structure['subject']

        incl_srv_mgd_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.RETURN_SRV_MGD_RES_URI),
        })
        omit_srv_mgd_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.RETURN_SRV_MGD_RES_URI),
        })

        assert incl_srv_mgd_resp.data == cont_resp.data

        incl_g = Graph().parse(data=incl_srv_mgd_resp.data, format='turtle')
        omit_g = Graph().parse(data=omit_srv_mgd_resp.data, format='turtle')

        for pred in {
            nsc['fcrepo'].created,
            nsc['fcrepo'].createdBy,
            nsc['fcrepo'].lastModified,
            nsc['fcrepo'].lastModifiedBy,
            nsc['ldp'].contains,
        }:
            assert set(incl_g[ cont_subject : pred : ])
            assert not set(omit_g[ cont_subject : pred : ])

        for type in {
                nsc['fcrepo'].Resource,
                nsc['ldp'].Container,
                nsc['ldp'].Resource,
        }:
            assert incl_g[ cont_subject : RDF.type : type ]
            assert not omit_g[ cont_subject : RDF.type : type ]


    def test_delete_no_tstone(self):
        '''
        Test the `no-tombstone` Prefer option.
        '''
        self.client.put('/ldp/test_delete_no_tstone01')
        self.client.put('/ldp/test_delete_no_tstone01/a')

        self.client.delete('/ldp/test_delete_no_tstone01', headers={
                'prefer' : 'no-tombstone'})

        resp = self.client.get('/ldp/test_delete_no_tstone01')
        assert resp.status_code == 404

        child_resp = self.client.get('/ldp/test_delete_no_tstone01/a')
        assert child_resp.status_code == 404

