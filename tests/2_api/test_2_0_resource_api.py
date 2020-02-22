import pdb
import pytest

from io import BytesIO
from uuid import uuid4

from rdflib import Literal, URIRef

from lakesuperior import env
from lakesuperior.api import resource as rsrc_api
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import (
        IncompatibleLdpTypeError, InvalidResourceError, ResourceNotExistsError,
        TombstoneError)
from lakesuperior.model.ldp.ldpr import Ldpr, RES_CREATED, RES_UPDATED
from lakesuperior.model.rdf.graph import Graph, from_rdf

txn_ctx = env.app_globals.rdf_store.txn_ctx


@pytest.fixture(scope='module')
def random_uuid():
    return str(uuid.uuid4())

@pytest.fixture
def dc_rdf():
    return b'''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX ldp: <http://www.w3.org/ns/ldp#>

    <> a ldp:DirectContainer ;
        dcterms:title "Direct Container" ;
        ldp:membershipResource <info:fcres/member> ;
        ldp:hasMemberRelation dcterms:relation .
    '''


@pytest.fixture
def ic_rdf():
    return b'''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX ldp: <http://www.w3.org/ns/ldp#>
    PREFIX ore: <http://www.openarchives.org/ore/terms/>

    <> a ldp:IndirectContainer ;
        dcterms:title "Indirect Container" ;
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
        with txn_ctx():
            assert gr[gr.uri : nsc['rdf'].type : nsc['ldp'].Resource ]
            assert not gr[
                gr.uri : nsc['dcterms'].title : Literal("Repository Root")
            ]


    def test_get_root_node(self):
        """
        Get the root node.

        The ``dcterms:title`` property should be included.
        """
        rsrc = rsrc_api.get('/')
        assert isinstance(rsrc, Ldpr)
        gr = rsrc.imr
        assert len(gr) == 10
        with txn_ctx():
            assert gr[gr.uri : nsc['rdf'].type : nsc['ldp'].Resource ]
            assert gr[
                gr.uri : nsc['dcterms'].title : Literal('Repository Root')]


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
        with txn_ctx():
            gr = from_rdf(
                data='<> a <http://ex.org/type#A> .', format='turtle',
                publicID=uri)
        evt, _ = rsrc_api.create_or_replace(uid, graph=gr)

        rsrc = rsrc_api.get(uid)
        with txn_ctx():
            assert rsrc.imr[
                    rsrc.uri : nsc['rdf'].type : URIRef('http://ex.org/type#A')]
            assert rsrc.imr[
                    rsrc.uri : nsc['rdf'].type : nsc['ldp'].RDFSource]


    def test_create_ldp_rs_literals(self):
        """
        Create an RDF resource (LDP-RS) containing different literal types.
        """
        uid = f'/{uuid4()}'
        uri = nsc['fcres'][uid]
        with txn_ctx():
            gr = from_rdf(
                data = '''
                <>
                  <urn:p:1> 1 ;
                  <urn:p:2> "Untyped Literal" ;
                  <urn:p:3> "Typed Literal"^^<http://www.w3.org/2001/XMLSchema#string> ;
                  <urn:p:4> "2019-09-26"^^<http://www.w3.org/2001/XMLSchema#date> ;
                  <urn:p:5> "Lang-tagged Literal"@en-US ;
                  .
                ''', format='turtle',
                publicID=uri)
        evt, _ = rsrc_api.create_or_replace(uid, graph=gr)

        rsrc = rsrc_api.get(uid)
        with txn_ctx():
            assert rsrc.imr[
                    rsrc.uri : URIRef('urn:p:1') :
                    Literal('1', datatype=nsc['xsd'].integer)]
            assert rsrc.imr[
                    rsrc.uri : URIRef('urn:p:2') : Literal('Untyped Literal')]
            assert rsrc.imr[
                    rsrc.uri : URIRef('urn:p:3') :
                    Literal('Typed Literal', datatype=nsc['xsd'].string)]
            assert rsrc.imr[
                    rsrc.uri : URIRef('urn:p:4') :
                    Literal('2019-09-26', datatype=nsc['xsd'].date)]
            assert rsrc.imr[
                    rsrc.uri : URIRef('urn:p:5') :
                    Literal('Lang-tagged Literal', lang='en-US')]


    def test_create_ldp_nr(self):
        """
        Create a non-RDF resource (LDP-NR).
        """
        uid = '/{}'.format(uuid4())
        data = b'Hello. This is some dummy content.'
        rsrc_api.create_or_replace(
                uid, stream=BytesIO(data), mimetype='text/plain')

        rsrc = rsrc_api.get(uid)
        with rsrc.imr.store.txn_ctx():
            assert rsrc.content.read() == data


    def test_replace_rsrc(self):
        uid = '/test_replace'
        uri = nsc['fcres'][uid]
        with txn_ctx():
            gr1 = from_rdf(
                data='<> a <http://ex.org/type#A> .', format='turtle',
                publicID=uri
            )
        evt, _ = rsrc_api.create_or_replace(uid, graph=gr1)
        assert evt == RES_CREATED

        rsrc = rsrc_api.get(uid)
        with txn_ctx():
            assert rsrc.imr[
                    rsrc.uri : nsc['rdf'].type : URIRef('http://ex.org/type#A')]
            assert rsrc.imr[
                    rsrc.uri : nsc['rdf'].type : nsc['ldp'].RDFSource]

        with txn_ctx():
            gr2 = from_rdf(
                data='<> a <http://ex.org/type#B> .', format='turtle',
                publicID=uri
            )
        #pdb.set_trace()
        evt, _ = rsrc_api.create_or_replace(uid, graph=gr2)
        assert evt == RES_UPDATED

        rsrc = rsrc_api.get(uid)
        with txn_ctx():
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
        with txn_ctx():
            gr = from_rdf(
                data='<> a <http://ex.org/type#A> .', format='turtle',
                publicID=nsc['fcres'][uid_rs]
            )

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

        with txn_ctx():
            gr = Graph(data=init_trp)
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc_api.update_delta(uid, remove_trp, add_trp)
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
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

        with txn_ctx():
            gr = Graph(data=init_trp)
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc_api.update_delta(uid, remove_trp, add_trp)
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
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
        with txn_ctx():
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
        dc_rsrc = rsrc_api.create(
                '/', 'test_dc_post', rdf_data=dc_rdf, rdf_fmt='turtle')

        member_rsrc = rsrc_api.get('/member')

        with txn_ctx():
            assert nsc['ldp'].Container in dc_rsrc.ldp_types
            assert nsc['ldp'].DirectContainer in dc_rsrc.ldp_types


    def test_create_ldp_dc_put(self, dc_rdf):
        """
        Create an LDP Direct Container via PUT.
        """
        dc_uid = '/test_dc_put01'
        _, dc_rsrc = rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        member_rsrc = rsrc_api.get('/member')

        with txn_ctx():
            assert nsc['ldp'].Container in dc_rsrc.ldp_types
            assert nsc['ldp'].DirectContainer in dc_rsrc.ldp_types


    def test_add_dc_member(self, dc_rdf):
        """
        Add members to a direct container and verify special properties.
        """
        dc_uid = '/test_dc_put02'
        _, dc_rsrc = rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        child_uid = rsrc_api.create(dc_uid).uid
        member_rsrc = rsrc_api.get('/member')

        with txn_ctx():
            assert member_rsrc.imr[
                member_rsrc.uri: nsc['dcterms'].relation: nsc['fcres'][child_uid]]


    def test_create_ldp_dc_defaults1(self):
        """
        Create an LDP Direct Container with default values.
        """
        dc_rdf = b'''
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX ldp: <http://www.w3.org/ns/ldp#>

        <> a ldp:DirectContainer ;
            ldp:membershipResource <info:fcres/member> .
        '''
        dc_uid = '/test_dc_defaults1'
        _, dc_rsrc = rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        child_uid = rsrc_api.create(dc_uid).uid
        member_rsrc = rsrc_api.get('/member')

        with txn_ctx():
            assert member_rsrc.imr[
                member_rsrc.uri: nsc['ldp'].member: nsc['fcres'][child_uid]
            ]


    def test_create_ldp_dc_defaults2(self):
        """
        Create an LDP Direct Container with default values.
        """
        dc_rdf = b'''
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX ldp: <http://www.w3.org/ns/ldp#>

        <> a ldp:DirectContainer ;
            ldp:hasMemberRelation dcterms:relation .
        '''
        dc_uid = '/test_dc_defaults2'
        _, dc_rsrc = rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        child_uid = rsrc_api.create(dc_uid).uid
        member_rsrc = rsrc_api.get(dc_uid)

        with txn_ctx():
            #import pdb; pdb.set_trace()
            assert member_rsrc.imr[
                member_rsrc.uri: nsc['dcterms'].relation:
                nsc['fcres'][child_uid]]


    def test_create_ldp_dc_defaults3(self):
        """
        Create an LDP Direct Container with default values.
        """
        dc_rdf = b'''
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX ldp: <http://www.w3.org/ns/ldp#>

        <> a ldp:DirectContainer .
        '''
        dc_uid = '/test_dc_defaults3'
        _, dc_rsrc = rsrc_api.create_or_replace(
                dc_uid, rdf_data=dc_rdf, rdf_fmt='turtle')

        child_uid = rsrc_api.create(dc_uid, None).uid
        member_rsrc = rsrc_api.get(dc_uid)

        with txn_ctx():
            assert member_rsrc.imr[
                member_rsrc.uri: nsc['ldp'].member: nsc['fcres'][child_uid]]


    def test_indirect_container(self, ic_rdf):
        """
        Create an indirect container and verify special properties.
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
        with txn_ctx():
            assert nsc['ldp'].Container in ic_rsrc.ldp_types
            assert nsc['ldp'].IndirectContainer in ic_rsrc.ldp_types
            assert nsc['ldp'].DirectContainer not in ic_rsrc.ldp_types

        member_rsrc = rsrc_api.get(member_uid)
        top_cont_rsrc = rsrc_api.get(cont_uid)
        with txn_ctx():
            assert top_cont_rsrc.imr[
                top_cont_rsrc.uri: nsc['dcterms'].relation:
                nsc['fcres'][target_uid]]


    # TODO WIP Complex test of all possible combinations of missing IC triples
    # falling back to default values.
    #def test_indirect_container_defaults(self):
    #    """
    #    Create an indirect container with various default values.
    #    """
    #    ic_rdf_base = b'''
    #    PREFIX dcterms: <http://purl.org/dc/terms/>
    #    PREFIX ldp: <http://www.w3.org/ns/ldp#>
    #    PREFIX ore: <http://www.openarchives.org/ore/terms/>

    #    <> a ldp:IndirectContainer ;
    #    '''
    #    ic_rdf_trp1 = '\nldp:membershipResource <info:fcres/top_container> ;'
    #    ic_rdf_trp2 = '\nldp:hasMemberRelation dcterms:relation ;'
    #    ic_rdf_trp3 = '\nldp:insertedContentRelation ore:proxyFor ;'

    #    ic_def_rdf = [
    #        ic_rdf_base + ic_rdf_trp1 + ic_trp2 + '\n.',
    #        ic_rdf_base + ic_rdf_trp1 + ic_trp3 + '\n.',
    #        ic_rdf_base + ic_rdf_trp2 + ic_trp3 + '\n.',
    #        ic_rdf_base + ic_rdf_trp1 + '\n.',
    #        ic_rdf_base + ic_rdf_trp2 + '\n.',
    #        ic_rdf_base + ic_rdf_trp3 + '\n.',
    #        ic_rdf_base + '\n.',
    #    ]

    #    target_uid = '/ic_target_def'
    #    rsrc_api.create_or_replace(target_uid)

    #    # Create several sets of indirect containers, each missing one or more
    #    # triples from the original graph, which should be replaced by default
    #    # values. All combinations are tried.
    #    for i, ic_rdf in enumerate(ic_def_rdf):
    #        cont_uid = f'/top_container_def{i}'
    #        ic_uid = '{}/test_ic'.format(cont_uid)
    #        member_uid = '{}/ic_member'.format(ic_uid)

    #        rsrc_api.create_or_replace(cont_uid)
    #        rsrc_api.create_or_replace(
    #            ic_uid, rdf_data=ic_rdf, rdf_fmt='turtle'
    #        )

    #        ic_member_p = (
    #            nsc['ore'].proxyFor if i in (1, 2, 5)
    #            else nsc['ldp'].memberSubject
    #        )
    #        # WIP
    #        #ic_member_o_uid = (
    #        #    'ic_target_def' if i in (1, 2, 5)
    #        #    else nsc['ldp'].memberSubject
    #        #)

    #        ic_member_rdf = b'''
    #        PREFIX ore: <http://www.openarchives.org/ore/terms/>
    #        <> ore:proxyFor <info:fcres/ic_target_def> .'''

    #        rsrc_api.create_or_replace(
    #                member_uid, rdf_data=ic_member_rdf, rdf_fmt='turtle')

    #        ic_rsrc = rsrc_api.get(ic_uid)
    #        with txn_ctx():
    #            assert nsc['ldp'].Container in ic_rsrc.ldp_types
    #            assert nsc['ldp'].IndirectContainer in ic_rsrc.ldp_types

    #    top_cont_rsrc = rsrc_api.get(cont_uid)

    #    for i, ic_rdf in enumerate(ic_def_rdf):
    #        member_rsrc = rsrc_api.get(member_uid)
    #        with txn_ctx():
    #            assert top_cont_rsrc.imr[
    #                top_cont_rsrc.uri: nsc['dcterms'].relation:
    #                nsc['fcres'][target_uid]]

    def test_user_data(self):
        '''
        Verify that only user-defined data are in user_data.
        '''
        data = b'''
        <> a <urn:t:1> ;
            <urn:p:1> "Property 1" ;
            <urn:p:2> <urn:o:2> .
        '''
        uid = f'/{uuid4()}'
        uri = nsc['fcres'][uid]

        rsrc_api.create_or_replace(uid, rdf_data=data, rdf_fmt='ttl')
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            ud_data = rsrc.user_data

            assert ud_data[uri: nsc['rdf'].type: URIRef('urn:t:1')]
            assert ud_data[uri: URIRef('urn:p:1'): Literal('Property 1')]
            assert ud_data[uri: URIRef('urn:p:2'): URIRef('urn:o:2')]
            assert not ud_data[uri: nsc['rdf'].type: nsc['ldp'].Resource]


    def test_types(self):
        '''
        Test server-managed and user-defined RDF types.
        '''
        data = b'''
        <> a <urn:t:1> , <urn:t:2> .
        '''
        uid = f'/{uuid4()}'
        uri = nsc['fcres'][uid]

        rsrc_api.create_or_replace(uid, rdf_data=data, rdf_fmt='ttl')
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert URIRef('urn:t:1') in rsrc.types
            assert URIRef('urn:t:1') in rsrc.user_types
            assert URIRef('urn:t:1') not in rsrc.ldp_types

            assert URIRef('urn:t:2') in rsrc.types
            assert URIRef('urn:t:2') in rsrc.user_types
            assert URIRef('urn:t:2') not in rsrc.ldp_types

            assert nsc['ldp'].Resource in rsrc.types
            assert nsc['ldp'].Resource not in rsrc.user_types
            assert nsc['ldp'].Resource in rsrc.ldp_types

            assert nsc['ldp'].Container in rsrc.types
            assert nsc['ldp'].Container not in rsrc.user_types
            assert nsc['ldp'].Container in rsrc.ldp_types


    def test_inbound_triples_ldprs(self):
        """ Test displaying of inbound triples for a LDP_RS. """
        src_uid = f'/{uuid4()}'
        src_uri = nsc['fcres'][src_uid]
        trg_uid = f'/{uuid4()}'
        trg_uri = nsc['fcres'][trg_uid]

        src_data = f'<> <urn:p:1> <{trg_uri}> .'.encode()
        trg_data = b'<> <urn:p:2> <urn:o:1> .'

        with txn_ctx(True):
            rsrc_api.create_or_replace(
                    trg_uid, rdf_data=trg_data, rdf_fmt='ttl')
            rsrc_api.create_or_replace(
                    src_uid, rdf_data=src_data, rdf_fmt='ttl')

        rsrc = rsrc_api.get(trg_uid, repr_options={'incl_inbound': True})

        with txn_ctx():
            assert (src_uri, URIRef('urn:p:1'), trg_uri) in rsrc.imr


    def test_inbound_triples_ldpnr(self):
        """ Test displaying of inbound triples for a LDP_NR. """
        src_uid = f'/{uuid4()}'
        src_uri = nsc['fcres'][src_uid]
        trg_uid = f'/{uuid4()}'
        trg_uri = nsc['fcres'][trg_uid]

        src_data = f'<> <urn:p:1> <{trg_uri}> .'.encode()
        trg_data = b'Some ASCII content.'

        with txn_ctx(True):
            rsrc_api.create_or_replace(
                    trg_uid, stream=BytesIO(trg_data), mimetype='text/plain')
            rsrc_api.create_or_replace(
                    src_uid, rdf_data=src_data, rdf_fmt='ttl')

        rsrc = rsrc_api.get(trg_uid, repr_options={'incl_inbound': True})

        with txn_ctx():
            assert (src_uri, URIRef('urn:p:1'), trg_uri) in rsrc.imr


@pytest.mark.usefixtures('db')
class TestRelativeUris:
    '''
    Test inserting and updating resources with relative URIs.
    '''
    def test_create_self_uri_rdf(self):
        """
        Create a resource with empty string ("self") URIs in the RDF body.
        """
        uid = '/reluri01'
        uri = nsc['fcres'][uid]
        data = '''
        <> a <urn:type:A> .
        <http://ex.org/external> <urn:pred:x> <> .
        '''
        rsrc_api.create_or_replace(uid, rdf_data=data, rdf_fmt='ttl')
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert rsrc.imr[uri: nsc['rdf']['type']: URIRef('urn:type:A')]
            assert rsrc.imr[
                URIRef('http://ex.org/external'): URIRef('urn:pred:x'): uri]


    def test_create_self_uri_graph(self):
        """
        Create a resource with empty string ("self") URIs in a RDFlib graph.
        """
        uid = '/reluri02'
        uri = nsc['fcres'][uid]
        gr = Graph()
        with txn_ctx():
            gr.add({
                (URIRef(''), nsc['rdf']['type'], URIRef('urn:type:A')),
                (
                    URIRef('http://ex.org/external'),
                    URIRef('urn:pred:x'), URIRef('')
                ),
            })
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert rsrc.imr[uri: nsc['rdf']['type']: URIRef('urn:type:A')]
            assert rsrc.imr[
                URIRef('http://ex.org/external'): URIRef('urn:pred:x'): uri]


    def test_create_hash_uri_rdf(self):
        """
        Create a resource with empty string ("self") URIs in the RDF body.
        """
        uid = '/reluri03'
        uri = nsc['fcres'][uid]
        data = '''
        <#hash1> a <urn:type:A> .
        <http://ex.org/external> <urn:pred:x> <#hash2> .
        '''
        rsrc_api.create_or_replace(uid, rdf_data=data, rdf_fmt='ttl')
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert rsrc.imr[
                URIRef(str(uri) + '#hash1'): nsc['rdf'].type:
                URIRef('urn:type:A')]
            assert rsrc.imr[
                URIRef('http://ex.org/external'): URIRef('urn:pred:x'):
                URIRef(str(uri) + '#hash2')]


    @pytest.mark.skip
    def test_create_hash_uri_graph(self):
        """
        Create a resource with empty string ("self") URIs in a RDFlib graph.
        """
        uid = '/reluri04'
        uri = nsc['fcres'][uid]
        gr = Graph()
        with txn_ctx():
            gr.add({
                (URIRef('#hash1'), nsc['rdf']['type'], URIRef('urn:type:A')),
                (
                    URIRef('http://ex.org/external'),
                    URIRef('urn:pred:x'), URIRef('#hash2')
                )
            })
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert rsrc.imr[
                URIRef(str(uri) + '#hash1'): nsc['rdf']['type']:
                URIRef('urn:type:A')]
            assert rsrc.imr[
                URIRef('http://ex.org/external'): URIRef('urn:pred:x'):
                URIRef(str(uri) + '#hash2')]


    @pytest.mark.skip(reason='RDFlib bug.')
    def test_create_child_uri_rdf(self):
        """
        Create a resource with empty string ("self") URIs in the RDF body.
        """
        uid = '/reluri05'
        uri = nsc['fcres'][uid]
        data = '''
        <child1> a <urn:type:A> .
        <http://ex.org/external> <urn:pred:x> <child2> .
        '''
        rsrc_api.create_or_replace(uid, rdf_data=data, rdf_fmt='ttl')
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert rsrc.imr[
                URIRef(str(uri) + '/child1'): nsc['rdf'].type:
                URIRef('urn:type:A')]
            assert rsrc.imr[
                URIRef('http://ex.org/external'): URIRef('urn:pred:x'):
                URIRef(str(uri) + '/child2')]


    @pytest.mark.skip(reason='RDFlib bug.')
    def test_create_child_uri_graph(self):
        """
        Create a resource with empty string ("self") URIs in the RDF body.
        """
        uid = '/reluri06'
        uri = nsc['fcres'][uid]
        gr = Graph()
        with txn_ctx():
            gr.add({
                (URIRef('child1'), nsc['rdf']['type'], URIRef('urn:type:A')),
                (
                    URIRef('http://ex.org/external'),
                    URIRef('urn:pred:x'), URIRef('child22')
                )
            })
        rsrc_api.create_or_replace(uid, graph=gr)
        rsrc = rsrc_api.get(uid)

        with txn_ctx():
            assert rsrc.imr[
                URIRef(str(uri) + '/child1'): nsc['rdf'].type:
                URIRef('urn:type:A')]
            assert rsrc.imr[
                URIRef('http://ex.org/external'): URIRef('urn:pred:x'):
                URIRef(str(uri) + '/child2')]



@pytest.mark.usefixtures('db')
class TestAdvancedDelete:
    '''
    Test resource version lifecycle.
    '''
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
        with txn_ctx():
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
        with txn_ctx():
            assert nsc['ldp'].Resource in parent_rsrc.ldp_types
        for i in range(3):
            child_rsrc = rsrc_api.get('{}/child{}'.format(uid, i))
            with txn_ctx():
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


    def test_hard_delete_descendants(self):
        """
        Forget a resource with all its descendants.
        """
        uid = '/test_hard_delete_descendants01'
        rsrc_api.create_or_replace(uid)
        for i in range(1, 4):
            rsrc_api.create_or_replace('{}/child{}'.format(uid, i))
            for j in range(i):
                rsrc_api.create_or_replace('{}/child{}/grandchild{}'.format(
                    uid, i, j))
        rsrc_api.delete(uid, False)
        with pytest.raises(ResourceNotExistsError):
            rsrc_api.get(uid)
        with pytest.raises(ResourceNotExistsError):
            rsrc_api.resurrect(uid)

        for i in range(1, 4):
            with pytest.raises(ResourceNotExistsError):
                rsrc_api.get('{}/child{}'.format(uid, i))
            with pytest.raises(ResourceNotExistsError):
                rsrc_api.resurrect('{}/child{}'.format(uid, i))

            for j in range(i):
                with pytest.raises(ResourceNotExistsError):
                    rsrc_api.get('{}/child{}/grandchild{}'.format(
                        uid, i, j))
                with pytest.raises(ResourceNotExistsError):
                    rsrc_api.resurrect('{}/child{}/grandchild{}'.format(
                        uid, i, j))



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
        #FIXME Without this, the test fails.
        #set(rsrc_api.get_version(uid, ver_uid))

        rsrc_api.update(uid, update_str)
        current = rsrc_api.get(uid)
        with txn_ctx():
            assert (
                (current.uri, nsc['dcterms'].title, Literal('Title #2.'))
                in current.imr)
            assert (
                (current.uri, nsc['dcterms'].title, Literal('Original title.'))
                not in current.imr)

        v1 = rsrc_api.get_version(uid, ver_uid)
        with txn_ctx():
            assert (
                (v1.uri, nsc['dcterms'].title, Literal('Original title.'))
                in set(v1))
            assert (
                (v1.uri, nsc['dcterms'].title, Literal('Title #2.'))
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
        with txn_ctx():
            assert (
                (rev.uri, nsc['dcterms'].title, Literal('Original title.'))
                in rev.imr)


    def test_versioning_children(self):
        """
        Test that children are not affected by version restoring.

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
        with txn_ctx():
            assert nsc['fcres'][ch1_uid] in rsrc.imr[
                    rsrc.uri : nsc['ldp'].contains]

        rsrc_api.create_or_replace(ch2_uid)
        rsrc = rsrc_api.get(uid)
        with txn_ctx():
            assert nsc['fcres'][ch2_uid] in rsrc.imr[
                    rsrc.uri : nsc['ldp'].contains]

        rsrc_api.revert_to_version(uid, ver_uid)
        rsrc = rsrc_api.get(uid)
        with txn_ctx():
            assert nsc['fcres'][ch1_uid] in rsrc.imr[
                    rsrc.uri : nsc['ldp'].contains]
            assert nsc['fcres'][ch2_uid] in rsrc.imr[
                    rsrc.uri : nsc['ldp'].contains]

