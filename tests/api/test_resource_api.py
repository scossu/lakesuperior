import pdb
import pytest

from io import BytesIO
from uuid import uuid4

from rdflib import Graph, Literal, URIRef

from lakesuperior import env
from lakesuperior.api import resource as rsrc_api
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import (
        IncompatibleLdpTypeError, InvalidResourceError, ResourceNotExistsError,
        TombstoneError)
from lakesuperior.globals import RES_CREATED, RES_UPDATED
from lakesuperior.model.ldpr import Ldpr
from lakesuperior.store.ldp_rs.metadata_store import MetadataStore


@pytest.fixture(scope='module')
def random_uuid():
    return str(uuid.uuid4())

@pytest.fixture
def dc_rdf():
    return b'''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX ldp: <http://www.w3.org/ns/ldp#>

    <> dcterms:title "Direct Container" ;
        ldp:membershipResource <info:fcres/member> ;
        ldp:hasMemberRelation dcterms:relation .
    '''


@pytest.fixture
def ic_rdf():
    return b'''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX ldp: <http://www.w3.org/ns/ldp#>
    PREFIX ore: <http://www.openarchives.org/ore/terms/>

    <> dcterms:title "Indirect Container" ;
        ldp:membershipResource <info:fcres/top_container> ;
        ldp:hasMemberRelation dcterms:relation ;
        ldp:insertedContentRelation ore:proxyFor .
    '''


@pytest.mark.usefixtures('db')
class TestResourceCRUD:
    '''
    Test interaction with the Resource API.
    '''
    def test_nodes_exist(self):
        """
        Verify whether nodes exist or not.
        """
        assert rsrc_api.exists('/') is True
        assert rsrc_api.exists('/{}'.format(uuid4())) is False


    def test_get_root_node_metadata(self):
        """
        Get the root node metadata.

        The ``dcterms:title`` property should NOT be included.
        """
        gr = rsrc_api.get_metadata('/')
        assert isinstance(gr, Graph)
        assert len(gr) == 9
        assert gr[gr.identifier : nsc['rdf'].type : nsc['ldp'].Resource ]
        assert not gr[gr.identifier : nsc['dcterms'].title : "Repository Root"]


    def test_get_root_node(self):
        """
        Get the root node.

        The ``dcterms:title`` property should be included.
        """
        rsrc = rsrc_api.get('/')
        assert isinstance(rsrc, Ldpr)
        gr = rsrc.imr
        assert len(gr) == 10
        assert gr[gr.identifier : nsc['rdf'].type : nsc['ldp'].Resource ]
        assert gr[
            gr.identifier : nsc['dcterms'].title : Literal('Repository Root')]


    def test_get_nonexisting_node(self):
        """
        Get a non-existing node.
        """
        with pytest.raises(ResourceNotExistsError):
            gr = rsrc_api.get('/{}'.format(uuid4()))


    def test_create_ldp_rs(self):
        """
        Create an RDF resource (LDP-RS) from a provided graph.
        """
        uid = '/rsrc_from_graph'
        uri = nsc['fcres'][uid]
        gr = Graph().parse(
            data='<> a <http://ex.org/type#A> .', format='turtle',
            publicID=uri)
        #pdb.set_trace()
        evt = rsrc_api.create_or_replace(uid, graph=gr)

        rsrc = rsrc_api.get(uid)
        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : URIRef('http://ex.org/type#A')]
        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : nsc['ldp'].RDFSource]


    def test_create_ldp_nr(self):
        """
        Create a non-RDF resource (LDP-NR).
        """
        uid = '/{}'.format(uuid4())
        data = b'Hello. This is some dummy content.'
        rsrc_api.create_or_replace(
                uid, stream=BytesIO(data), mimetype='text/plain')

        rsrc = rsrc_api.get(uid)
        assert rsrc.content.read() == data


    def test_replace_rsrc(self):
        uid = '/test_replace'
        uri = nsc['fcres'][uid]
        gr1 = Graph().parse(
            data='<> a <http://ex.org/type#A> .', format='turtle',
            publicID=uri)
        evt = rsrc_api.create_or_replace(uid, graph=gr1)
        assert evt == RES_CREATED

        rsrc = rsrc_api.get(uid)
        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : URIRef('http://ex.org/type#A')]
        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : nsc['ldp'].RDFSource]

        gr2 = Graph().parse(
            data='<> a <http://ex.org/type#B> .', format='turtle',
            publicID=uri)
        #pdb.set_trace()
        evt = rsrc_api.create_or_replace(uid, graph=gr2)
        assert evt == RES_UPDATED

        rsrc = rsrc_api.get(uid)
        assert not rsrc.imr[
                rsrc.uri : nsc['rdf'].type : URIRef('http://ex.org/type#A')]
        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : URIRef('http://ex.org/type#B')]
        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : nsc['ldp'].RDFSource]


    def test_replace_incompatible_type(self):
        """
        Verify replacing resources with incompatible type.

        Replacing a LDP-NR with a LDP-RS, or vice versa, should fail.
        """
        uid_rs = '/test_incomp_rs'
        uid_nr = '/test_incomp_nr'
        data = b'mock binary content'
        gr = Graph().parse(
            data='<> a <http://ex.org/type#A> .', format='turtle',
            publicID=nsc['fcres'][uid_rs])

        rsrc_api.create_or_replace(uid_rs, graph=gr)
        rsrc_api.create_or_replace(
            uid_nr, stream=BytesIO(data), mimetype='text/plain')

        with pytest.raises(IncompatibleLdpTypeError):
            rsrc_api.create_or_replace(uid_nr, graph=gr)

        with pytest.raises(IncompatibleLdpTypeError):
            rsrc_api.create_or_replace(
                uid_rs, stream=BytesIO(data), mimetype='text/plain')

        with pytest.raises(IncompatibleLdpTypeError):
            rsrc_api.create_or_replace(uid_nr)


    def test_delta_update(self):
        """
        Update a resource with two sets of add and remove triples.
        """
        uid = '/test_delta_patch'
        uri = nsc['fcres'][uid]
        init_trp = {
            (URIRef(uri), nsc['rdf'].type, nsc['foaf'].Person),
            (URIRef(uri), nsc['foaf'].name, Literal('Joe Bob')),
        }
        remove_trp = {
            (URIRef(uri), nsc['rdf'].type, nsc['foaf'].Person),
        }
        add_trp = {
            (URIRef(uri), nsc['rdf'].type, nsc['foaf'].Organization),
        }

        gr = Graph()
        gr += init_trp
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc_api.update_delta(uid, remove_trp, add_trp)
        rsrc = rsrc_api.get(uid)

        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : nsc['foaf'].Organization]
        assert rsrc.imr[rsrc.uri : nsc['foaf'].name : Literal('Joe Bob')]
        assert not rsrc.imr[
                rsrc.uri : nsc['rdf'].type : nsc['foaf'].Person]


    def test_delta_update_wildcard(self):
        """
        Update a resource using wildcard modifiers.
        """
        uid = '/test_delta_patch_wc'
        uri = nsc['fcres'][uid]
        init_trp = {
            (URIRef(uri), nsc['rdf'].type, nsc['foaf'].Person),
            (URIRef(uri), nsc['foaf'].name, Literal('Joe Bob')),
            (URIRef(uri), nsc['foaf'].name, Literal('Joe Average Bob')),
            (URIRef(uri), nsc['foaf'].name, Literal('Joe 12oz Bob')),
        }
        remove_trp = {
            (URIRef(uri), nsc['foaf'].name, None),
        }
        add_trp = {
            (URIRef(uri), nsc['foaf'].name, Literal('Joan Knob')),
        }

        gr = Graph()
        gr += init_trp
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc_api.update_delta(uid, remove_trp, add_trp)
        rsrc = rsrc_api.get(uid)

        assert rsrc.imr[
                rsrc.uri : nsc['rdf'].type : nsc['foaf'].Person]
        assert rsrc.imr[rsrc.uri : nsc['foaf'].name : Literal('Joan Knob')]
        assert not rsrc.imr[rsrc.uri : nsc['foaf'].name : Literal('Joe Bob')]
        assert not rsrc.imr[
            rsrc.uri : nsc['foaf'].name : Literal('Joe Average Bob')]
        assert not rsrc.imr[
            rsrc.uri : nsc['foaf'].name : Literal('Joe 12oz Bob')]


    def test_sparql_update(self):
        """
        Update a resource using a SPARQL Update string.

        Use a mix of relative and absolute URIs.
        """
        uid = '/test_sparql'
        rdf_data = b'<> <http://purl.org/dc/terms/title> "Original title." .'
        update_str = '''DELETE {
        <> <http://purl.org/dc/terms/title> "Original title." .
        } INSERT {
        <> <http://purl.org/dc/terms/title> "Title #2." .
        <info:fcres/test_sparql>
          <http://purl.org/dc/terms/title> "Title #3." .
        <#h1> <http://purl.org/dc/terms/title> "This is a hash." .
        } WHERE {
        }'''
        rsrc_api.create_or_replace(uid, rdf_data=rdf_data, rdf_fmt='turtle')
        ver_uid = rsrc_api.create_version(uid, 'v1').split('fcr:versions/')[-1]

        rsrc = rsrc_api.update(uid, update_str)
        assert (
            (rsrc.uri, nsc['dcterms'].title, Literal('Original title.'))
            not in set(rsrc.imr))
        assert (
            (rsrc.uri, nsc['dcterms'].title, Literal('Title #2.'))
            in set(rsrc.imr))
        assert (
            (rsrc.uri, nsc['dcterms'].title, Literal('Title #3.'))
            in set(rsrc.imr))
        assert ((
                URIRef(str(rsrc.uri) + '#h1'),
                nsc['dcterms'].title, Literal('This is a hash.'))
            in set(rsrc.imr))


    def test_create_ldp_dc_post(self, dc_rdf):
        """
        Create an LDP Direct Container via POST.
        """
        rsrc_api.create_or_replace('/member')
        dc_uid = rsrc_api.create(
                '/', 'test_dc_post', rdf_data=dc_rdf, rdf_fmt='turtle')

        dc_rsrc = rsrc_api.get(dc_uid)
        member_rsrc = rsrc_api.get('/member')

        assert nsc['ldp'].Container in dc_rsrc.ldp_types
        assert nsc['ldp'].DirectContainer in dc_rsrc.ldp_types


    def test_create_ldp_dc_put(self, dc_rdf):
        """
        Create an LDP Direct Container via PUT.
        """
        dc_uid = '/test_dc_put01'
        rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        dc_rsrc = rsrc_api.get(dc_uid)
        member_rsrc = rsrc_api.get('/member')

        assert nsc['ldp'].Container in dc_rsrc.ldp_types
        assert nsc['ldp'].DirectContainer in dc_rsrc.ldp_types


    def test_add_dc_member(self, dc_rdf):
        """
        Add members to a direct container and verify special properties.
        """
        dc_uid = '/test_dc_put02'
        rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        dc_rsrc = rsrc_api.get(dc_uid)
        child_uid = rsrc_api.create(dc_uid, None)
        member_rsrc = rsrc_api.get('/member')

        assert member_rsrc.imr[
            member_rsrc.uri: nsc['dcterms'].relation: nsc['fcres'][child_uid]]


    def test_indirect_container(self, ic_rdf):
        """
        Create an indirect container verify special properties.
        """
        cont_uid = '/top_container'
        ic_uid = '{}/test_ic'.format(cont_uid)
        member_uid = '{}/ic_member'.format(ic_uid)
        target_uid = '/ic_target'
        ic_member_rdf = b'''
        PREFIX ore: <http://www.openarchives.org/ore/terms/>
        <> ore:proxyFor <info:fcres/ic_target> .'''

        rsrc_api.create_or_replace(cont_uid)
        rsrc_api.create_or_replace(target_uid)
        rsrc_api.create_or_replace(ic_uid, rdf_data=ic_rdf, rdf_fmt='turtle')
        rsrc_api.create_or_replace(
                member_uid, rdf_data=ic_member_rdf, rdf_fmt='turtle')

        ic_rsrc = rsrc_api.get(ic_uid)
        assert nsc['ldp'].Container in ic_rsrc.ldp_types
        assert nsc['ldp'].IndirectContainer in ic_rsrc.ldp_types
        assert nsc['ldp'].DirectContainer not in ic_rsrc.ldp_types

        member_rsrc = rsrc_api.get(member_uid)
        top_cont_rsrc = rsrc_api.get(cont_uid)
        assert top_cont_rsrc.imr[
            top_cont_rsrc.uri: nsc['dcterms'].relation:
            nsc['fcres'][target_uid]]


    def test_soft_delete(self):
        """
        Soft-delete (bury) a resource.
        """
        uid = '/test_soft_delete01'
        rsrc_api.create_or_replace(uid)
        rsrc_api.delete(uid)
        with pytest.raises(TombstoneError):
            rsrc_api.get(uid)


    def test_resurrect(self):
        """
        Restore (resurrect) a soft-deleted resource.
        """
        uid = '/test_soft_delete02'
        rsrc_api.create_or_replace(uid)
        rsrc_api.delete(uid)
        rsrc_api.resurrect(uid)

        rsrc = rsrc_api.get(uid)
        assert nsc['ldp'].Resource in rsrc.ldp_types


    def test_hard_delete(self):
        """
        Hard-delete (forget) a resource.
        """
        uid = '/test_hard_delete01'
        rsrc_api.create_or_replace(uid)
        rsrc_api.delete(uid, False)
        with pytest.raises(ResourceNotExistsError):
            rsrc_api.get(uid)
        with pytest.raises(ResourceNotExistsError):
            rsrc_api.resurrect(uid)


    def test_delete_children(self):
        """
        Soft-delete a resource with children.
        """
        uid = '/test_soft_delete_children01'
        rsrc_api.create_or_replace(uid)
        for i in range(3):
            rsrc_api.create_or_replace('{}/child{}'.format(uid, i))
        rsrc_api.delete(uid)
        with pytest.raises(TombstoneError):
            rsrc_api.get(uid)
        for i in range(3):
            with pytest.raises(TombstoneError):
                rsrc_api.get('{}/child{}'.format(uid, i))
            # Cannot resurrect children of a tombstone.
            with pytest.raises(TombstoneError):
                rsrc_api.resurrect('{}/child{}'.format(uid, i))


    def test_resurrect_children(self):
        """
        Resurrect a resource with its children.

        This uses fixtures from the previous test.
        """
        uid = '/test_soft_delete_children01'
        rsrc_api.resurrect(uid)
        parent_rsrc = rsrc_api.get(uid)
        assert nsc['ldp'].Resource in parent_rsrc.ldp_types
        for i in range(3):
            child_rsrc = rsrc_api.get('{}/child{}'.format(uid, i))
            assert nsc['ldp'].Resource in child_rsrc.ldp_types


    def test_hard_delete_children(self):
        """
        Hard-delete (forget) a resource with its children.

        This uses fixtures from the previous test.
        """
        uid = '/test_hard_delete_children01'
        rsrc_api.create_or_replace(uid)
        for i in range(3):
            rsrc_api.create_or_replace('{}/child{}'.format(uid, i))
        rsrc_api.delete(uid, False)
        with pytest.raises(ResourceNotExistsError):
            rsrc_api.get(uid)
        with pytest.raises(ResourceNotExistsError):
            rsrc_api.resurrect(uid)

        for i in range(3):
            with pytest.raises(ResourceNotExistsError):
                rsrc_api.get('{}/child{}'.format(uid, i))
            with pytest.raises(ResourceNotExistsError):
                rsrc_api.resurrect('{}/child{}'.format(uid, i))


    def test_checksum(self):
        """
        Verify that a checksum is created and updated appropriately.
        """
        root_cksum1 = env.app_globals.md_store.get_checksum(nsc['fcres']['/'])
        uid = '/test_checksum'
        rsrc_api.create_or_replace(uid)

        root_cksum2 = env.app_globals.md_store.get_checksum(nsc['fcres']['/'])
        cksum1 = env.app_globals.md_store.get_checksum(nsc['fcres'][uid])

        assert len(cksum1)
        assert root_cksum1 != root_cksum2

        rsrc_api.update(
                uid,
                'DELETE {} INSERT {<> a <http://ex.org/ns#Hello> .} WHERE {}')

        cksum2 = env.app_globals.md_store.get_checksum(nsc['fcres'][uid])

        assert cksum1 != cksum2

        rsrc_api.delete(uid)

        cksum3 = env.app_globals.md_store.get_checksum(nsc['fcres'][uid])

        assert cksum3 is None



@pytest.mark.usefixtures('db')
class TestResourceVersioning:
    '''
    Test resource version lifecycle.
    '''
    def test_create_version(self):
        """
        Create a version snapshot.
        """
        uid = '/test_version1'
        rdf_data = b'<> <http://purl.org/dc/terms/title> "Original title." .'
        update_str = '''DELETE {
        <> <http://purl.org/dc/terms/title> "Original title." .
        } INSERT {
        <> <http://purl.org/dc/terms/title> "Title #2." .
        } WHERE {
        }'''
        rsrc_api.create_or_replace(uid, rdf_data=rdf_data, rdf_fmt='turtle')
        ver_uid = rsrc_api.create_version(uid, 'v1').split('fcr:versions/')[-1]

        rsrc_api.update(uid, update_str)
        current = rsrc_api.get(uid)
        assert (
            (current.uri, nsc['dcterms'].title, Literal('Title #2.'))
            in current.imr)
        assert (
            (current.uri, nsc['dcterms'].title, Literal('Original title.'))
            not in current.imr)

        v1 = rsrc_api.get_version(uid, ver_uid)
        assert (
            (v1.identifier, nsc['dcterms'].title, Literal('Original title.'))
            in set(v1))
        assert (
            (v1.identifier, nsc['dcterms'].title, Literal('Title #2.'))
            not in set(v1))


    def test_revert_to_version(self):
        """
        Test reverting to a previous version.

        Uses assets from previous test.
        """
        uid = '/test_version1'
        ver_uid = 'v1'
        rsrc_api.revert_to_version(uid, ver_uid)
        rev = rsrc_api.get(uid)
        assert (
            (rev.uri, nsc['dcterms'].title, Literal('Original title.'))
            in rev.imr)


    def test_versioning_children(self):
        """
        Test that children are not affected by version restoring.

        This test does the following:

        1. create parent resource
        2. Create child 1
        3. Version parent
        4. Create child 2
        5. Restore parent to previous version
        6. Verify that restored version still has 2 children
        """
        uid = '/test_version_children'
        ver_uid = 'v1'
        ch1_uid = '{}/kid_a'.format(uid)
        ch2_uid = '{}/kid_b'.format(uid)
        rsrc_api.create_or_replace(uid)
        rsrc_api.create_or_replace(ch1_uid)
        ver_uid = rsrc_api.create_version(uid, ver_uid).split('fcr:versions/')[-1]
        rsrc = rsrc_api.get(uid)
        assert nsc['fcres'][ch1_uid] in rsrc.imr.objects(
                rsrc.uri, nsc['ldp'].contains)

        rsrc_api.create_or_replace(ch2_uid)
        rsrc = rsrc_api.get(uid)
        assert nsc['fcres'][ch2_uid] in rsrc.imr.objects(
                rsrc.uri, nsc['ldp'].contains)

        rsrc_api.revert_to_version(uid, ver_uid)
        rsrc = rsrc_api.get(uid)
        assert nsc['fcres'][ch1_uid] in rsrc.imr.objects(
                rsrc.uri, nsc['ldp'].contains)
        assert nsc['fcres'][ch2_uid] in rsrc.imr.objects(
                rsrc.uri, nsc['ldp'].contains)

