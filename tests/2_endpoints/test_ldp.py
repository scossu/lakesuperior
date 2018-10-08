import pdb
import pytest
import uuid

from base64 import b64encode
from hashlib import sha1

from flask import g
from rdflib import Graph
from rdflib.compare import isomorphic
from rdflib.namespace import RDF
from rdflib.term import Literal, URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr


@pytest.fixture(scope='module')
def random_uuid():
    return str(uuid.uuid4())


@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestLdp:
    """
    Test HTTP interaction with LDP endpoint.
    """
    def test_get_root_node(self):
        """
        Get the root node from two different endpoints.

        The test triplestore must be initialized, hence the `db` fixture.
        """
        ldp_resp = self.client.get('/ldp')
        rest_resp = self.client.get('/rest')

        assert ldp_resp.status_code == 200
        assert rest_resp.status_code == 200


    def test_put_empty_resource(self, random_uuid):
        """
        Check response headers for a PUT operation with empty payload.
        """
        resp = self.client.put('/ldp/new_resource')
        assert resp.status_code == 201
        assert resp.data == bytes(
                '{}/new_resource'.format(g.webroot), 'utf-8')


    def test_put_existing_resource(self, random_uuid):
        """
        Trying to PUT an existing resource should return a 204 if the payload
        is empty.
        """
        path = '/ldp/nonidempotent01'
        put1_resp = self.client.put(path)
        assert put1_resp.status_code == 201

        assert self.client.get(path).status_code == 200

        put2_resp = self.client.put(path)
        with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
            put2_resp = self.client.put(
                    path, data=f, content_type='text/turtle')
        assert put2_resp.status_code == 204

        put2_resp = self.client.put(path)
        assert put2_resp.status_code == 204


    def test_put_tree(self, client):
        """
        PUT a resource with several path segments.

        The test should create intermediate path segments that are LDPCs,
        accessible to PUT or POST.
        """
        path = '/ldp/test_tree/a/b/c/d/e/f/g'
        self.client.put(path)

        assert self.client.get(path).status_code == 200
        assert self.client.get('/ldp/test_tree/a/b/c').status_code == 200

        assert self.client.post('/ldp/test_tree/a/b').status_code == 201
        with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
            put_int_resp = self.client.put(
                    'ldp/test_tree/a', data=f, content_type='text/turtle')
        assert put_int_resp.status_code == 204
        # @TODO More thorough testing of contents


    def test_put_nested_tree(self, client):
        """
        Verify that containment is set correctly in nested hierarchies.

        First put a new hierarchy and verify that the root node is its
        container; then put another hierarchy under it and verify that the
        first hierarchy is the container of the second one.
        """
        uuid1 = 'test_nested_tree/a/b/c/d'
        uuid2 = uuid1 + '/e/f/g'
        path1 = '/ldp/' + uuid1
        path2 = '/ldp/' + uuid2

        self.client.put(path1)

        cont1_data = self.client.get('/ldp').data
        gr1 = Graph().parse(data=cont1_data, format='turtle')
        assert gr1[ URIRef(g.webroot + '/') : nsc['ldp'].contains : \
                URIRef(g.webroot + '/test_nested_tree') ]

        self.client.put(path2)

        cont2_data = self.client.get(path1).data
        gr2 = Graph().parse(data=cont2_data, format='turtle')
        assert gr2[ URIRef(g.webroot + '/' + uuid1) : \
                nsc['ldp'].contains : \
                URIRef(g.webroot + '/' + uuid1 + '/e') ]


    def test_put_ldp_rs(self, client):
        """
        PUT a resource with RDF payload and verify.
        """
        with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
            self.client.put('/ldp/ldprs01', data=f, content_type='text/turtle')

        resp = self.client.get('/ldp/ldprs01',
                headers={'accept' : 'text/turtle'})
        assert resp.status_code == 200

        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert URIRef('http://vocab.getty.edu/ontology#Subject') in \
                gr.objects(None, RDF.type)


    def test_put_ldp_nr(self, rnd_img):
        """
        PUT a resource with binary payload and verify checksums.
        """
        rnd_img['content'].seek(0)
        resp = self.client.put('/ldp/ldpnr01', data=rnd_img['content'],
                headers={
                    'Content-Type': 'image/png',
                    'Content-Disposition' : 'attachment; filename={}'.format(
                    rnd_img['filename'])})
        assert resp.status_code == 201

        resp = self.client.get(
                '/ldp/ldpnr01', headers={'accept' : 'image/png'})
        assert resp.status_code == 200
        assert sha1(resp.data).hexdigest() == rnd_img['hash']


    def test_put_ldp_nr_multipart(self, rnd_img):
        """
        PUT a resource with a multipart/form-data payload.
        """
        rnd_img['content'].seek(0)
        resp = self.client.put(
            '/ldp/ldpnr02',
            data={
                'file': (
                    rnd_img['content'], rnd_img['filename'],
                    'image/png',
                )
            }
        )
        assert resp.status_code == 201

        resp = self.client.get(
                '/ldp/ldpnr02', headers={'accept' : 'image/png'})
        assert resp.status_code == 200
        assert sha1(resp.data).hexdigest() == rnd_img['hash']


    def test_get_ldp_nr(self, rnd_img):
        """
        PUT a resource with binary payload and test various retieval methods.
        """
        uid = '/ldpnr03'
        path = '/ldp' + uid
        content = b'This is some exciting content.'
        resp = self.client.put(path, data=content,
                headers={
                    'Content-Type': 'text/plain',
                    'Content-Disposition' : 'attachment; filename=exciting.txt'})
        assert resp.status_code == 201
        uri = g.webroot + uid

        # Content retrieval methods.
        resp_bin1 = self.client.get(path)
        assert resp_bin1.status_code == 200
        assert resp_bin1.data == content

        resp_bin2 = self.client.get(path, headers={'accept' : 'text/plain'})
        assert resp_bin2.status_code == 200
        assert resp_bin2.data == content

        resp_bin3 = self.client.get(path + '/fcr:content')
        assert resp_bin3.status_code == 200
        assert resp_bin3.data == content

        # Metadata retrieval methods.
        resp_md1 = self.client.get(path, headers={'accept' : 'text/turtle'})
        assert resp_md1.status_code == 200
        gr1 = Graph().parse(data=resp_md1.data, format='text/turtle')
        assert gr1[ URIRef(uri) : nsc['rdf'].type : nsc['ldp'].Resource]

        resp_md2 = self.client.get(path + '/fcr:metadata')
        assert resp_md2.status_code == 200
        gr2 = Graph().parse(data=resp_md2.data, format='text/turtle')
        assert isomorphic(gr1, gr2)



    def test_put_mismatched_ldp_rs(self, rnd_img):
        """
        Verify MIME type / LDP mismatch.
        PUT a LDP-RS, then PUT a LDP-NR on the same location and verify it
        fails.
        """
        path = '/ldp/' + str(uuid.uuid4())

        rnd_img['content'].seek(0)
        ldp_nr_resp = self.client.put(path, data=rnd_img['content'],
                headers={
                    'Content-Disposition' : 'attachment; filename={}'.format(
                    rnd_img['filename'])})

        assert ldp_nr_resp.status_code == 201

        with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
            ldp_rs_resp = self.client.put(path, data=f,
                    content_type='text/turtle')

        assert ldp_rs_resp.status_code == 415


    def test_put_mismatched_ldp_nr(self, rnd_img):
        """
        Verify MIME type / LDP mismatch.
        PUT a LDP-NR, then PUT a LDP-RS on the same location and verify it
        fails.
        """
        path = '/ldp/' + str(uuid.uuid4())

        with open('tests/data/marcel_duchamp_single_subject.ttl', 'rb') as f:
            ldp_rs_resp = self.client.put(path, data=f,
                    content_type='text/turtle')

        assert ldp_rs_resp.status_code == 201

        rnd_img['content'].seek(0)
        ldp_nr_resp = self.client.put(path, data=rnd_img['content'],
                headers={
                    'Content-Disposition' : 'attachment; filename={}'.format(
                    rnd_img['filename'])})

        assert ldp_nr_resp.status_code == 415


    def test_missing_reference(self, client):
        """
        PUT a resource with RDF payload referencing a non-existing in-repo
        resource.
        """
        self.client.get('/ldp')
        data = '''
        PREFIX ns: <http://example.org#>
        PREFIX res: <http://example-source.org/res/>
        <> ns:p1 res:bogus ;
          ns:p2 <{0}> ;
          ns:p3 <{0}/> ;
          ns:p4 <{0}/nonexistent> .
        '''.format(g.webroot)
        put_rsp = self.client.put('/ldp/test_missing_ref', data=data, headers={
            'content-type': 'text/turtle'})
        assert put_rsp.status_code == 201

        resp = self.client.get('/ldp/test_missing_ref',
                headers={'accept' : 'text/turtle'})
        assert resp.status_code == 200

        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert URIRef('http://example-source.org/res/bogus') in \
                gr.objects(None, URIRef('http://example.org#p1'))
        assert URIRef(g.webroot + '/') in (
                gr.objects(None, URIRef('http://example.org#p2')))
        assert URIRef(g.webroot + '/') in (
                gr.objects(None, URIRef('http://example.org#p3')))
        assert URIRef(g.webroot + '/nonexistent') not in (
                gr.objects(None, URIRef('http://example.org#p4')))


    def test_post_resource(self, client):
        """
        Check response headers for a POST operation with empty payload.
        """
        res = self.client.post('/ldp/')
        assert res.status_code == 201
        assert 'Location' in res.headers


    def test_post_ldp_nr(self, rnd_img):
        """
        POST a resource with binary payload and verify checksums.
        """
        rnd_img['content'].seek(0)
        resp = self.client.post('/ldp/', data=rnd_img['content'],
                headers={
                    'slug': 'ldpnr04',
                    'Content-Type': 'image/png',
                    'Content-Disposition' : 'attachment; filename={}'.format(
                    rnd_img['filename'])})
        assert resp.status_code == 201

        resp = self.client.get(
                '/ldp/ldpnr04', headers={'accept' : 'image/png'})
        assert resp.status_code == 200
        assert sha1(resp.data).hexdigest() == rnd_img['hash']


    def test_post_slug(self):
        """
        Verify that a POST with slug results in the expected URI only if the
        resource does not exist already.
        """
        slug01_resp = self.client.post('/ldp', headers={'slug' : 'slug01'})
        assert slug01_resp.status_code == 201
        assert slug01_resp.headers['location'] == \
                g.webroot + '/slug01'

        slug02_resp = self.client.post('/ldp', headers={'slug' : 'slug01'})
        assert slug02_resp.status_code == 201
        assert slug02_resp.headers['location'] != \
                g.webroot + '/slug01'


    def test_post_404(self):
        """
        Verify that a POST to a non-existing parent results in a 404.
        """
        assert self.client.post('/ldp/{}'.format(uuid.uuid4()))\
                .status_code == 404


    def test_post_409(self, rnd_img):
        """
        Verify that you cannot POST to a binary resource.
        """
        rnd_img['content'].seek(0)
        self.client.put('/ldp/post_409', data=rnd_img['content'], headers={
                'Content-Disposition' : 'attachment; filename={}'.format(
                rnd_img['filename'])})
        assert self.client.post('/ldp/post_409').status_code == 409


    def test_patch_root(self):
        """
        Test patching root node.
        """
        path = '/ldp/'
        self.client.get(path)
        uri = g.webroot + '/'

        with open('tests/data/sparql_update/simple_insert.sparql') as data:
            resp = self.client.patch(path,
                    data=data,
                    headers={'content-type' : 'application/sparql-update'})

        assert resp.status_code == 204

        resp = self.client.get(path)
        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert gr[ URIRef(uri) : nsc['dc'].title : Literal('Hello') ]


    def test_patch(self):
        """
        Test patching a resource.
        """
        path = '/ldp/test_patch01'
        self.client.put(path)

        uri = g.webroot + '/test_patch01'

        with open('tests/data/sparql_update/simple_insert.sparql') as data:
            resp = self.client.patch(path,
                    data=data,
                    headers={'content-type' : 'application/sparql-update'})

        assert resp.status_code == 204

        resp = self.client.get(path)
        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert gr[ URIRef(uri) : nsc['dc'].title : Literal('Hello') ]

        self.client.patch(path,
                data=open('tests/data/sparql_update/delete+insert+where.sparql'),
                headers={'content-type' : 'application/sparql-update'})

        resp = self.client.get(path)
        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert gr[ URIRef(uri) : nsc['dc'].title : Literal('Ciao') ]


    def test_patch_ssr(self):
        """
        Test patching a resource violating the single-subject rule.
        """
        path = '/ldp/test_patch_ssr'
        self.client.put(path)

        uri = g.webroot + '/test_patch_ssr'

        nossr_qry = 'INSERT { <http://bogus.org> a <urn:ns:A> . } WHERE {}'
        abs_qry = 'INSERT {{ <{}> a <urn:ns:A> . }} WHERE {{}}'.format(uri)
        frag_qry = 'INSERT {{ <{}#frag> a <urn:ns:A> . }} WHERE {{}}'\
                .format(uri)

        # @TODO Leave commented until a decision is made about SSR.
        assert self.client.patch(
            path, data=nossr_qry,
            headers={'content-type': 'application/sparql-update'}
        ).status_code == 204

        assert self.client.patch(
            path, data=abs_qry,
            headers={'content-type': 'application/sparql-update'}
        ).status_code == 204

        assert self.client.patch(
            path, data=frag_qry,
            headers={'content-type': 'application/sparql-update'}
        ).status_code == 204


    def test_patch_ldp_nr_metadata(self):
        """
        Test patching a LDP-NR metadata resource from the fcr:metadata URI.
        """
        path = '/ldp/ldpnr01'

        with open('tests/data/sparql_update/simple_insert.sparql') as data:
            self.client.patch(path + '/fcr:metadata',
                    data=data,
                    headers={'content-type' : 'application/sparql-update'})

        resp = self.client.get(path + '/fcr:metadata')
        assert resp.status_code == 200

        uri = g.webroot + '/ldpnr01'
        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert gr[URIRef(uri) : nsc['dc'].title : Literal('Hello')]

        with open(
                'tests/data/sparql_update/delete+insert+where.sparql') as data:
            patch_resp = self.client.patch(path + '/fcr:metadata',
                    data=data,
                    headers={'content-type' : 'application/sparql-update'})
        assert patch_resp.status_code == 204

        resp = self.client.get(path + '/fcr:metadata')
        assert resp.status_code == 200

        gr = Graph().parse(data=resp.data, format='text/turtle')
        assert gr[ URIRef(uri) : nsc['dc'].title : Literal('Ciao') ]


    def test_patch_ldpnr(self):
        """
        Verify that a direct PATCH to a LDP-NR results in a 415.
        """
        with open(
                'tests/data/sparql_update/delete+insert+where.sparql') as data:
            patch_resp = self.client.patch('/ldp/ldpnr01',
                    data=data,
                    headers={'content-type': 'application/sparql-update'})
        assert patch_resp.status_code == 415


    def test_patch_invalid_mimetype(self, rnd_img):
        """
        Verify that a PATCH using anything other than an
        `application/sparql-update` MIME type results in an error.
        """
        self.client.put('/ldp/test_patch_invalid_mimetype')
        rnd_img['content'].seek(0)
        ldpnr_resp = self.client.patch('/ldp/ldpnr01/fcr:metadata',
                data=rnd_img,
                headers={'content-type' : 'image/jpeg'})

        ldprs_resp = self.client.patch('/ldp/test_patch_invalid_mimetype',
                data=b'Hello, I\'m not a SPARQL update.',
                headers={'content-type' : 'text/plain'})

        assert ldprs_resp.status_code == ldpnr_resp.status_code == 415


    def test_delete(self):
        """
        Test delete response codes.
        """
        self.client.put('/ldp/test_delete01')
        delete_resp = self.client.delete('/ldp/test_delete01')
        assert delete_resp.status_code == 204

        bogus_delete_resp = self.client.delete('/ldp/test_delete101')
        assert bogus_delete_resp.status_code == 404


    def test_tombstone(self):
        """
        Test tombstone behaviors.

        For POST on a tombstone, check `test_resurrection`.
        """
        tstone_resp = self.client.get('/ldp/test_delete01')
        assert tstone_resp.status_code == 410
        assert tstone_resp.headers['Link'] == \
                '<{}/test_delete01/fcr:tombstone>; rel="hasTombstone"'\
                .format(g.webroot)

        tstone_path = '/ldp/test_delete01/fcr:tombstone'
        assert self.client.get(tstone_path).status_code == 405
        assert self.client.put(tstone_path).status_code == 405
        assert self.client.delete(tstone_path).status_code == 204

        assert self.client.get('/ldp/test_delete01').status_code == 404


    def test_delete_recursive(self):
        """
        Test response codes for resources deleted recursively and their
        tombstones.
        """
        child_suffixes = ('a', 'a/b', 'a/b/c', 'a1', 'a1/b1')
        self.client.put('/ldp/test_delete_recursive01')
        for cs in child_suffixes:
            self.client.put('/ldp/test_delete_recursive01/{}'.format(cs))

        assert self.client.delete(
                '/ldp/test_delete_recursive01').status_code == 204

        tstone_resp = self.client.get('/ldp/test_delete_recursive01')
        assert tstone_resp.status_code == 410
        assert tstone_resp.headers['Link'] == \
            '<{}/test_delete_recursive01/fcr:tombstone>; rel="hasTombstone"'\
            .format(g.webroot)

        for cs in child_suffixes:
            child_tstone_resp = self.client.get(
                    '/ldp/test_delete_recursive01/{}'.format(cs))
            assert child_tstone_resp.status_code == tstone_resp.status_code
            assert 'Link' not in child_tstone_resp.headers.keys()


    def test_put_fragments(self):
        """
        Test the correct handling of fragment URIs on PUT and GET.
        """
        with open('tests/data/fragments.ttl', 'rb') as f:
            self.client.put(
                '/ldp/test_fragment01',
                headers={
                    'Content-Type' : 'text/turtle',
                },
                data=f
            )

        rsp = self.client.get('/ldp/test_fragment01')
        gr = Graph().parse(data=rsp.data, format='text/turtle')
        assert gr[
                URIRef(g.webroot + '/test_fragment01#hash1')
                : URIRef('http://ex.org/p2') : URIRef('http://ex.org/o2')]


    def test_patch_fragments(self):
        """
        Test the correct handling of fragment URIs on PATCH.
        """
        self.client.put('/ldp/test_fragment_patch')

        with open('tests/data/fragments_insert.sparql', 'rb') as f:
            self.client.patch(
                '/ldp/test_fragment_patch',
                headers={
                    'Content-Type' : 'application/sparql-update',
                },
                data=f
            )
        ins_rsp = self.client.get('/ldp/test_fragment_patch')
        ins_gr = Graph().parse(data=ins_rsp.data, format='text/turtle')
        assert ins_gr[
                URIRef(g.webroot + '/test_fragment_patch#hash1234')
                : URIRef('http://ex.org/p3') : URIRef('http://ex.org/o3')]

        with open('tests/data/fragments_delete.sparql', 'rb') as f:
            self.client.patch(
                '/ldp/test_fragment_patch',
                headers={
                    'Content-Type' : 'application/sparql-update',
                },
                data=f
            )
        del_rsp = self.client.get('/ldp/test_fragment_patch')
        del_gr = Graph().parse(data=del_rsp.data, format='text/turtle')
        assert not del_gr[
                URIRef(g.webroot + '/test_fragment_patch#hash1234')
                : URIRef('http://ex.org/p3') : URIRef('http://ex.org/o3')]


@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestMimeType:
    """
    Test ``Accept`` headers and input & output formats.
    """
    def test_accept(self):
        """
        Verify the default serialization method.
        """
        accept_list = {
            ('', 'text/turtle'),
            ('text/turtle', 'text/turtle'),
            ('application/rdf+xml', 'application/rdf+xml'),
            ('application/n-triples', 'application/n-triples'),
            ('application/bogus', 'text/turtle'),
            (
                'application/rdf+xml;q=0.5,application/n-triples;q=0.7',
                'application/n-triples'),
            (
                'application/rdf+xml;q=0.5,application/bogus;q=0.7',
                'application/rdf+xml'),
            ('application/rdf+xml;q=0.5,text/n3;q=0.7', 'text/n3'),
            (
                'application/rdf+xml;q=0.5,application/ld+json;q=0.7',
                'application/ld+json'),
        }
        for mimetype, fmt in accept_list:
            rsp = self.client.get('/ldp', headers={'Accept': mimetype})
            assert rsp.mimetype == fmt
            gr = Graph(identifier=g.webroot + '/').parse(
                    data=rsp.data, format=fmt)

            assert nsc['fcrepo'].RepositoryRoot in set(gr.objects())


    def test_provided_rdf(self):
        """
        Test several input RDF serialiation formats.
        """
        self.client.get('/ldp')
        gr = Graph()
        gr.add((
            URIRef(g.webroot + '/test_mimetype'),
            nsc['dcterms'].title, Literal('Test MIME type.')))
        test_list = {
            'application/n-triples',
            'application/rdf+xml',
            'text/n3',
            'text/turtle',
            'application/ld+json',
        }
        for mimetype in test_list:
            rdf_data = gr.serialize(format=mimetype)
            self.client.put('/ldp/test_mimetype', data=rdf_data, headers={
                'content-type': mimetype})

            rsp = self.client.get('/ldp/test_mimetype')
            rsp_gr = Graph(identifier=g.webroot + '/test_mimetype').parse(
                    data=rsp.data, format='text/turtle')

            assert (
                    URIRef(g.webroot + '/test_mimetype'),
                    nsc['dcterms'].title, Literal('Test MIME type.')) in rsp_gr



@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestPrefHeader:
    """
    Test various combinations of `Prefer` header.
    """
    @pytest.fixture(scope='class')
    def cont_structure(self):
        """
        Create a container structure to be used for subsequent requests.
        """
        parent_path = '/ldp/test_parent'
        self.client.put(parent_path)
        self.client.put(parent_path + '/child1')
        self.client.put(parent_path + '/child2')
        self.client.put(parent_path + '/child3')

        return {
            'path' : parent_path,
            'response' : self.client.get(parent_path),
        }


    def test_put_prefer_handling(self, random_uuid):
        """
        Trying to PUT an existing resource should:

        - Return a 204 if the payload is empty
        - Return a 204 if the payload is RDF, server-managed triples are
          included and the 'Prefer' header is set to 'handling=lenient'
        - Return a 412 (ServerManagedTermError) if the payload is RDF,
          server-managed triples are included and handling is set to 'strict',
          or not set.
        """
        path = '/ldp/put_pref_header01'
        assert self.client.put(path).status_code == 201
        assert self.client.get(path).status_code == 200
        assert self.client.put(path).status_code == 204

        # Default handling is strict.
        with open('tests/data/rdf_payload_w_srv_mgd_trp.ttl', 'rb') as f:
            rsp_default = self.client.put(
                path,
                headers={
                    'Content-Type' : 'text/turtle',
                },
                data=f
            )
        assert rsp_default.status_code == 412

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


    # @HOLD Embed children is debated.
    def _disabled_test_embed_children(self, cont_structure):
        """
        verify the "embed children" prefer header.
        """
        self.client.get('/ldp')
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = URIRef(g.webroot + '/test_parent')

        #minimal_resp = self.client.get(parent_path, headers={
        #    'Prefer' : 'return=minimal',
        #})

        incl_embed_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.EMBED_CHILD_RES_URI),
        })
        omit_embed_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.EMBED_CHILD_RES_URI),
        })

        default_gr = Graph().parse(data=cont_resp.data, format='turtle')
        incl_gr = Graph().parse(
                data=incl_embed_children_resp.data, format='turtle')
        omit_gr = Graph().parse(
                data=omit_embed_children_resp.data, format='turtle')

        assert isomorphic(omit_gr, default_gr)

        children = set(incl_gr[cont_subject : nsc['ldp'].contains])
        assert len(children) == 3

        children = set(incl_gr[cont_subject : nsc['ldp'].contains])
        for child_uri in children:
            assert set(incl_gr[ child_uri : : ])
            assert not set(omit_gr[ child_uri : : ])


    def test_return_children(self, cont_structure):
        """
        verify the "return children" prefer header.
        """
        self.client.get('/ldp')
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = URIRef(g.webroot + '/test_parent')

        incl_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.RETURN_CHILD_RES_URI),
        })
        omit_children_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.RETURN_CHILD_RES_URI),
        })

        default_gr = Graph().parse(data=cont_resp.data, format='turtle')
        incl_gr = Graph().parse(data=incl_children_resp.data, format='turtle')
        omit_gr = Graph().parse(data=omit_children_resp.data, format='turtle')

        assert isomorphic(incl_gr, default_gr)

        children = incl_gr[cont_subject : nsc['ldp'].contains]
        for child_uri in children:
            assert not omit_gr[cont_subject : nsc['ldp'].contains : child_uri]


    def test_inbound_rel(self, cont_structure):
        """
        verify the "inbound relationships" prefer header.
        """
        self.client.put('/ldp/test_target')
        data = '<> <http://ex.org/ns#shoots> <{}> .'.format(
                g.webroot + '/test_target')
        self.client.put('/ldp/test_shooter', data=data,
                headers={'Content-Type': 'text/turtle'})

        cont_resp = self.client.get('/ldp/test_target')
        incl_inbound_resp = self.client.get('/ldp/test_target', headers={
            'Prefer' : 'return=representation; include="{}"'\
                    .format(Ldpr.RETURN_INBOUND_REF_URI),
        })
        omit_inbound_resp = self.client.get('/ldp/test_target', headers={
            'Prefer' : 'return=representation; omit="{}"'\
                    .format(Ldpr.RETURN_INBOUND_REF_URI),
        })

        default_gr = Graph().parse(data=cont_resp.data, format='turtle')
        incl_gr = Graph().parse(data=incl_inbound_resp.data, format='turtle')
        omit_gr = Graph().parse(data=omit_inbound_resp.data, format='turtle')

        subject = URIRef(g.webroot + '/test_target')
        inbd_subject = URIRef(g.webroot + '/test_shooter')
        assert isomorphic(omit_gr, default_gr)
        assert len(set(incl_gr[inbd_subject : : ])) == 1
        assert incl_gr[
            inbd_subject : URIRef('http://ex.org/ns#shoots') : subject]
        assert not len(set(omit_gr[inbd_subject : :]))


    def test_srv_mgd_triples(self, cont_structure):
        """
        verify the "server managed triples" prefer header.
        """
        self.client.get('/ldp')
        parent_path = cont_structure['path']
        cont_resp = cont_structure['response']
        cont_subject = URIRef(g.webroot + '/test_parent')

        incl_srv_mgd_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; include={}'\
                    .format(Ldpr.RETURN_SRV_MGD_RES_URI),
        })
        omit_srv_mgd_resp = self.client.get(parent_path, headers={
            'Prefer' : 'return=representation; omit={}'\
                    .format(Ldpr.RETURN_SRV_MGD_RES_URI),
        })

        default_gr = Graph().parse(data=cont_resp.data, format='turtle')
        incl_gr = Graph().parse(data=incl_srv_mgd_resp.data, format='turtle')
        omit_gr = Graph().parse(data=omit_srv_mgd_resp.data, format='turtle')

        assert isomorphic(incl_gr, default_gr)

        for pred in {
            nsc['fcrepo'].created,
            nsc['fcrepo'].createdBy,
            nsc['fcrepo'].lastModified,
            nsc['fcrepo'].lastModifiedBy,
            nsc['ldp'].contains,
        }:

            assert set(incl_gr[ cont_subject : pred : ])
            assert not set(omit_gr[ cont_subject : pred : ])

        for type in {
                nsc['fcrepo'].Resource,
                nsc['ldp'].Container,
                nsc['ldp'].Resource,
        }:
            assert incl_gr[ cont_subject : RDF.type : type ]
            assert not omit_gr[ cont_subject : RDF.type : type ]


    def test_delete_no_tstone(self):
        """
        Test the `no-tombstone` Prefer option.
        """
        self.client.put('/ldp/test_delete_no_tstone01')
        self.client.put('/ldp/test_delete_no_tstone01/a')

        self.client.delete('/ldp/test_delete_no_tstone01', headers={
                'prefer' : 'no-tombstone'})

        resp = self.client.get('/ldp/test_delete_no_tstone01')
        assert resp.status_code == 404

        child_resp = self.client.get('/ldp/test_delete_no_tstone01/a')
        assert child_resp.status_code == 404



#@pytest.mark.usefixtures('client_class')
#@pytest.mark.usefixtures('db')
#class TestDigest:
#    """
#    Test digest and ETag handling.
#    """
#    @pytest.mark.skip(reason='TODO Need to implement async digest queue')
#    def test_digest_post(self):
#        """
#        Test ``Digest`` and ``ETag`` headers on resource POST.
#        """
#        resp = self.client.post('/ldp/')
#        assert 'Digest' in resp.headers
#        assert 'ETag' in resp.headers
#        assert (
#                b64encode(bytes.fromhex(
#                    resp.headers['ETag'].replace('W/', '')
#                    )).decode('ascii') ==
#                resp.headers['Digest'].replace('SHA256=', ''))
#
#
#    @pytest.mark.skip(reason='TODO Need to implement async digest queue')
#    def test_digest_put(self):
#        """
#        Test ``Digest`` and ``ETag`` headers on resource PUT.
#        """
#        resp_put = self.client.put('/ldp/test_digest_put')
#        assert 'Digest' in resp_put.headers
#        assert 'ETag' in resp_put.headers
#        assert (
#                b64encode(bytes.fromhex(
#                    resp_put.headers['ETag'].replace('W/', '')
#                    )).decode('ascii') ==
#                resp_put.headers['Digest'].replace('SHA256=', ''))
#
#        resp_get = self.client.get('/ldp/test_digest_put')
#        assert 'Digest' in resp_get.headers
#        assert 'ETag' in resp_get.headers
#        assert (
#                b64encode(bytes.fromhex(
#                    resp_get.headers['ETag'].replace('W/', '')
#                    )).decode('ascii') ==
#                resp_get.headers['Digest'].replace('SHA256=', ''))
#
#
#    @pytest.mark.skip(reason='TODO Need to implement async digest queue')
#    def test_digest_patch(self):
#        """
#        Verify that the digest and ETag change on resource change.
#        """
#        path = '/ldp/test_digest_patch'
#        self.client.put(path)
#        rsp1 = self.client.get(path)
#
#        self.client.patch(
#                path, data=b'DELETE {} INSERT {<> a <http://ex.org/Test> .} '
#                b'WHERE {}',
#                headers={'Content-Type': 'application/sparql-update'})
#        rsp2 = self.client.get(path)
#
#        assert rsp1.headers['ETag'] != rsp2.headers['ETag']
#        assert rsp1.headers['Digest'] != rsp2.headers['Digest']


@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestVersion:
    """
    Test version creation, retrieval and deletion.
    """
    def test_create_versions(self):
        """
        Test that POSTing multiple times to fcr:versions creates the
        'hasVersions' triple and yields multiple version snapshots.
        """
        self.client.put('/ldp/test_version')
        create_rsp = self.client.post('/ldp/test_version/fcr:versions')

        assert create_rsp.status_code == 201

        rsrc_rsp = self.client.get('/ldp/test_version')
        rsrc_gr = Graph().parse(data=rsrc_rsp.data, format='turtle')
        assert len(set(rsrc_gr[: nsc['fcrepo'].hasVersions :])) == 1

        info_rsp = self.client.get('/ldp/test_version/fcr:versions')
        assert info_rsp.status_code == 200
        info_gr = Graph().parse(data=info_rsp.data, format='turtle')
        assert len(set(info_gr[: nsc['fcrepo'].hasVersion :])) == 1

        self.client.post('/ldp/test_version/fcr:versions')
        info2_rsp = self.client.get('/ldp/test_version/fcr:versions')

        info2_gr = Graph().parse(data=info2_rsp.data, format='turtle')
        assert len(set(info2_gr[: nsc['fcrepo'].hasVersion :])) == 2


    def test_version_with_slug(self):
        """
        Test a version with a slug.
        """
        self.client.put('/ldp/test_version_slug')
        create_rsp = self.client.post('/ldp/test_version_slug/fcr:versions',
            headers={'slug' : 'v1'})
        new_ver_uri = create_rsp.headers['Location']
        assert new_ver_uri == g.webroot + '/test_version_slug/fcr:versions/v1'

        info_rsp = self.client.get('/ldp/test_version_slug/fcr:versions')
        info_gr = Graph().parse(data=info_rsp.data, format='turtle')
        assert info_gr[
            URIRef(new_ver_uri) :
            nsc['fcrepo'].hasVersionLabel :
            Literal('v1')]


    def test_dupl_version(self):
        """
        Make sure that two POSTs with the same slug result in two different
        versions.
        """
        path = '/ldp/test_duplicate_slug'
        self.client.put(path)
        v1_rsp = self.client.post(path + '/fcr:versions',
            headers={'slug' : 'v1'})
        v1_uri = v1_rsp.headers['Location']

        dup_rsp = self.client.post(path + '/fcr:versions',
            headers={'slug' : 'v1'})
        dup_uri = dup_rsp.headers['Location']

        assert v1_uri != dup_uri


    @pytest.mark.skip(
            reason='TODO Reverting from version and resurrecting is not fully '
            'functional.')
    def test_revert_version(self):
        """
        Take a version snapshot, update a resource, and then revert to the
        previous vresion.
        """
        rsrc_path = '/ldp/test_revert_version'
        payload1 = '<> <urn:demo:p1> <urn:demo:o1> .'
        payload2 = '<> <urn:demo:p1> <urn:demo:o2> .'

        self.client.put(rsrc_path, headers={
            'content-type': 'text/turtle'}, data=payload1)
        self.client.post(
                rsrc_path + '/fcr:versions', headers={'slug': 'v1'})

        v1_rsp = self.client.get(rsrc_path)
        v1_gr = Graph().parse(data=v1_rsp.data, format='turtle')
        assert v1_gr[
            URIRef(g.webroot + '/test_revert_version')
            : URIRef('urn:demo:p1')
            : URIRef('urn:demo:o1')
        ]

        self.client.put(rsrc_path, headers={
            'content-type': 'text/turtle'}, data=payload2)

        v2_rsp = self.client.get(rsrc_path)
        v2_gr = Graph().parse(data=v2_rsp.data, format='turtle')
        assert v2_gr[
            URIRef(g.webroot + '/test_revert_version')
            : URIRef('urn:demo:p1')
            : URIRef('urn:demo:o2')
        ]

        self.client.patch(rsrc_path + '/fcr:versions/v1')

        revert_rsp = self.client.get(rsrc_path)
        revert_gr = Graph().parse(data=revert_rsp.data, format='turtle')
        assert revert_gr[
            URIRef(g.webroot + '/test_revert_version')
            : URIRef('urn:demo:p1')
            : URIRef('urn:demo:o1')
        ]


    def test_resurrection(self):
        """
        Delete and then resurrect a resource.

        Make sure that the resource is resurrected to the latest version.
        """
        path = '/ldp/test_lazarus'
        self.client.put(path)

        self.client.post(path + '/fcr:versions', headers={'slug': 'v1'})
        self.client.put(
            path, headers={'content-type': 'text/turtle'},
            data=b'<> <urn:demo:p1> <urn:demo:o1> .')
        self.client.post(path + '/fcr:versions', headers={'slug': 'v2'})
        self.client.put(
            path, headers={'content-type': 'text/turtle'},
            data=b'<> <urn:demo:p1> <urn:demo:o2> .')

        self.client.delete(path)

        assert self.client.get(path).status_code == 410

        self.client.post(path + '/fcr:tombstone')

        laz_data = self.client.get(path).data
        laz_gr = Graph().parse(data=laz_data, format='turtle')
        assert laz_gr[
            URIRef(g.webroot + '/test_lazarus')
            : URIRef('urn:demo:p1')
            : URIRef('urn:demo:o2')
        ]
