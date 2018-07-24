import pytest

from os import path
from shutil import rmtree

from rdflib import Namespace, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI
from rdflib.namespace import RDF, RDFS

from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore


@pytest.fixture(scope='class')
def store():
    store = LmdbStore('/tmp/test_lmdbstore')
    yield store
    store.close()
    store.destroy()


def _clean(res):
    return {r[0] for r in res}


@pytest.mark.usefixtures('store')
class TestStoreInit:
    '''
    Tests for intializing and shutting down store and transactions.
    '''
    def test_open_close(self):
        '''
        Test opening, closing and destroying a store.
        '''
        env_path = '/tmp/test_lmdbstore_init'
        tmpstore = LmdbStore(env_path)
        assert tmpstore.is_open
        tmpstore.close()
        assert not tmpstore.is_open
        tmpstore.destroy()
        assert not path.exists(env_path)
        assert not path.exists(env_path + '-lock')


    def test_txn(self, store):
        '''
        Test opening and closing the main transaction.
        '''
        store.begin(True)
        assert store.is_txn_open
        store.commit()
        assert not store.is_txn_open
        store.begin(True)
        store.abort()
        assert not store.is_txn_open


    def test_ctx_mgr(self, store):
        '''
        Test enclosing a transaction in a context.
        '''
        with store.txn_ctx() as txn:
            assert store.is_txn_open
            assert not store.is_txn_rw
        assert not store.is_txn_open

        with store.txn_ctx(True) as txn:
            assert store.is_txn_open
            assert store.is_txn_rw
        assert not store.is_txn_open
        assert not store.is_txn_rw

        try:
            with store.txn_ctx() as txn:
                raise RuntimeError
        except RuntimeError:
            assert not store.is_txn_open


    def test_rollback(self, store):
        '''
        Test rolling back a transaction.
        '''
        try:
            with store.txn_ctx(True) as txn:
                store.add((
                    URIRef('urn:nogo:s'), URIRef('urn:nogo:p'),
                    URIRef('urn:nogo:o')))
                raise RuntimeError # This should roll back the transaction.
        except RuntimeError:
            pass

        with store.txn_ctx() as txn:
            res = set(store.triples((None, None, None)))
        assert len(res) == 0


@pytest.mark.usefixtures('store')
class TestBasicOps:
    '''
    High-level tests for basic operations.
    '''
    def test_create_triple(self, store):
        '''
        Test creation of a single triple.
        '''
        trp = (
            URIRef('urn:test:s'), URIRef('urn:test:p'), URIRef('urn:test:o'))
        with store.txn_ctx(True) as txn:
            store.add(trp)

        with store.txn_ctx() as txn:
            res1 = set(store.triples((None, None, None)))
            res2 = set(store.triples(trp))
            assert len(res1) == 1
            assert len(res2) == 1
            clean_res1 = _clean(res1)
            clean_res2 = _clean(res2)
            assert trp in clean_res1 & clean_res2


    def test_triple_match_1bound(self, store):
        '''
        Test triple patterns matching one bound term.
        '''
        with store.txn_ctx() as txn:
            res1 = set(store.triples((URIRef('urn:test:s'), None, None)))
            res2 = set(store.triples((None, URIRef('urn:test:p'), None)))
            res3 = set(store.triples((None, None, URIRef('urn:test:o'))))
            assert _clean(res1) == {(
                URIRef('urn:test:s'), URIRef('urn:test:p'),
                URIRef('urn:test:o'))}
            assert _clean(res2) == _clean(res1)
            assert _clean(res3) == _clean(res2)


    def test_triple_match_2bound(self, store):
        '''
        Test triple patterns matching two bound terms.
        '''
        with store.txn_ctx() as txn:
            res1 = set(store.triples(
                (URIRef('urn:test:s'), URIRef('urn:test:p'), None)))
            res2 = set(store.triples(
                (URIRef('urn:test:s'), None, URIRef('urn:test:o'))))
            res3 = set(store.triples(
                (None, URIRef('urn:test:p'), URIRef('urn:test:o'))))
            assert _clean(res1) == {(
                URIRef('urn:test:s'),
                URIRef('urn:test:p'), URIRef('urn:test:o'))}
            assert _clean(res2) == _clean(res1)
            assert _clean(res3) == _clean(res2)


    def test_triple_match_3bound(self, store):
        '''
        Test triple patterns matching 3 bound terms (exact match).
        '''
        with store.txn_ctx() as txn:
            pattern = (
                URIRef('urn:test:s'), URIRef('urn:test:p'),
                URIRef('urn:test:o'))
            res1 = set(store.triples(pattern))
            assert _clean(res1) == {pattern}


    def test_triple_no_match_1bound(self, store):
        '''
        Test empty matches with 1 bound term.
        '''
        with store.txn_ctx(True) as txn:
            store.add((
                URIRef('urn:test:s'),
                URIRef('urn:test:p2'), URIRef('urn:test:o2')))
            store.add((
                URIRef('urn:test:s3'),
                URIRef('urn:test:p3'), URIRef('urn:test:o3')))
            res1 = set(store.triples((None, None, None)))
            assert len(res1) == 3

            res1 = set(store.triples((URIRef('urn:test:s2'), None, None)))
            res2 = set(store.triples((None, URIRef('urn:test:p4'), None)))
            res3 = set(store.triples((None, None, URIRef('urn:test:o4'))))

            assert len(res1) == len(res2) == len(res3) == 0



    def test_triple_no_match_2bound(self, store):
        '''
        Test empty matches with 2 bound terms.
        '''
        with store.txn_ctx(True) as txn:
            res1 = set(store.triples(
                (URIRef('urn:test:s2'), URIRef('urn:test:p'), None)))
            res2 = set(store.triples(
                (URIRef('urn:test:s3'), None, URIRef('urn:test:o'))))
            res3 = set(store.triples(
                (None, URIRef('urn:test:p3'), URIRef('urn:test:o2'))))

            assert len(res1) == len(res2) == len(res3) == 0


    def test_triple_no_match_3bound(self, store):
        '''
        Test empty matches with 3 bound terms.
        '''
        with store.txn_ctx(True) as txn:
            res1 = set(store.triples((
                URIRef('urn:test:s2'), URIRef('urn:test:p3'),
                URIRef('urn:test:o2'))))

            assert len(res1) == 0


    def test_remove(self, store):
        '''
        Test removing one or more triples.
        '''
        with store.txn_ctx(True) as txn:
            store.remove((URIRef('urn:test:s3'),
                    URIRef('urn:test:p3'), URIRef('urn:test:o3')))

            res1 = set(store.triples((None, None, None)))
            assert len(res1) == 2

            store.remove((URIRef('urn:test:s'), None, None))
            res2 = set(store.triples((None, None, None)))
            assert len(res2) == 0


@pytest.mark.usefixtures('store')
class TestBindings:
    '''
    Tests for namespace bindings.
    '''
    @pytest.fixture
    def bindings(self):
        return (
            ('ns1', Namespace('http://test.org/ns#')),
            ('ns2', Namespace('http://my_org.net/ns#')),
            ('ns3', Namespace('urn:test:')),
            ('ns4', Namespace('info:myinst/graph#')),
        )


    def test_ns(self, store, bindings):
        '''
        Test namespace bindings.
        '''
        with store.txn_ctx(True) as txn:
            for b in bindings:
                store.bind(*b)

            nslist = list(store.namespaces())
            assert len(nslist) == len(bindings)

            for i in range(len(bindings)):
                assert nslist[i] == bindings[i]


    def test_ns2pfx(self, store, bindings):
        '''
        Test namespace to prefix conversion.
        '''
        with store.txn_ctx(True) as txn:
            for b in bindings:
                pfx, ns = b
                assert store.namespace(pfx) == ns


    def test_pfx2ns(self, store, bindings):
        '''
        Test namespace to prefix conversion.
        '''
        with store.txn_ctx(True) as txn:
            for b in bindings:
                pfx, ns = b
                assert store.prefix(ns) == pfx


@pytest.mark.usefixtures('store')
class TestContext:
    '''
    Tests for context handling.
    '''
    def test_add_graph(self, store):
        '''
        Test creating an empty and a non-empty graph.
        '''
        gr_uri = URIRef('urn:bogus:graph#a')

        with store.txn_ctx(True) as txn:
            store.add_graph(gr_uri)
            assert gr_uri in {gr.identifier for gr in store.contexts()}


    def test_empty_context(self, store):
        '''
        Test creating and deleting empty contexts.
        '''
        gr_uri = URIRef('urn:bogus:empty#a')

        with store.txn_ctx(True) as txn:
            store.add_graph(gr_uri)
            assert gr_uri in {gr.identifier for gr in store.contexts()}
            store.remove_graph(gr_uri)
            assert gr_uri not in {gr.identifier for gr in store.contexts()}


    def test_add_trp_to_ctx(self, store):
        '''
        Test adding triples to a graph.
        '''
        gr_uri = URIRef('urn:bogus:graph#a') # From previous test
        gr2_uri = URIRef('urn:bogus:graph#b') # Never created before
        trp1 = (URIRef('urn:s:1'), URIRef('urn:p:1'), URIRef('urn:o:1'))
        trp2 = (URIRef('urn:s:2'), URIRef('urn:p:2'), URIRef('urn:o:2'))
        trp3 = (URIRef('urn:s:3'), URIRef('urn:p:3'), URIRef('urn:o:3'))
        trp4 = (URIRef('urn:s:4'), URIRef('urn:p:4'), URIRef('urn:o:4'))

        with store.txn_ctx(True) as txn:
            store.add(trp1, gr_uri)
            store.add(trp2, gr_uri)
            store.add(trp2, gr_uri) # Duplicate; dropped.
            store.add(trp2, None) # Goes to the default graph.
            store.add(trp3, gr2_uri)
            store.add(trp3, gr_uri)
            store.add(trp4) # Goes to the default graph.

            assert len(set(store.triples((None, None, None)))) == 4
            assert len(set(store.triples((None, None, None),
                RDFLIB_DEFAULT_GRAPH_URI))) == 2
            assert len(set(store.triples((None, None, None), gr_uri))) == 3
            assert len(set(store.triples((None, None, None), gr2_uri))) == 1

            assert gr2_uri in {gr.identifier for gr in store.contexts()}
            assert trp1 in _clean(store.triples((None, None, None)))
            assert trp1 not in _clean(store.triples((None, None, None),
                    RDFLIB_DEFAULT_GRAPH_URI))
            assert trp2 in _clean(store.triples((None, None, None), gr_uri))
            assert trp2 in _clean(store.triples((None, None, None)))
            assert trp3 in _clean(store.triples((None, None, None), gr2_uri))
            assert trp3 not in _clean(store.triples((None, None, None),
                    RDFLIB_DEFAULT_GRAPH_URI))


    def test_delete_from_ctx(self, store):
        '''
        Delete triples from a named graph and from the default graph.
        '''
        gr_uri = URIRef('urn:bogus:graph#a')
        gr2_uri = URIRef('urn:bogus:graph#b')

        with store.txn_ctx(True) as txn:
            store.remove((None, None, None), gr2_uri)
            assert len(set(store.triples((None, None, None), gr2_uri))) == 0
            assert len(set(store.triples((None, None, None), gr_uri))) == 3

        with store.txn_ctx(True) as txn:
            store.remove((URIRef('urn:s:1'), None, None))
            assert len(set(store.triples((None, None, None), gr_uri))) == 2
            assert len(set(store.triples((None, None, None)))) == 3

        with store.txn_ctx(True) as txn:
            store.remove((URIRef('urn:s:4'), None, None),
                    RDFLIB_DEFAULT_GRAPH_URI)
            assert len(set(store.triples((None, None, None)))) == 2

        with store.txn_ctx(True) as txn:
            store.remove((None, None, None))
            assert len(set(store.triples((None, None, None)))) == 0
            assert len(set(store.triples((None, None, None), gr_uri))) == 0
            assert len(store) == 0


@pytest.mark.usefixtures('store')
class TestTransactions:
    '''
    Tests for transaction handling.
    '''
    # @TODO Test concurrent reads and writes.
    pass


#@pytest.mark.usefixtures('store')
#class TestRdflib:
#    '''
#    Test case adapted from
#    http://rdflib.readthedocs.io/en/stable/univrdfstore.html#interface-test-cases
#    '''
#
#    @pytest.fixture
#    def sample_gr(self):
#        return Graph().parse('''
#        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
#        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
#        @prefix : <http://test/> .
#        {:a :b :c; a :foo} => {:a :d :c} .
#        _:foo a rdfs:Class .
#        :a :d :c .
#        ''', format='n3')
#
#    def _test_basic(self, sample_gr):
#        with store.txn_ctx() as txn:
#            implies = URIRef("http://www.w3.org/2000/10/swap/log#implies")
#            a = URIRef('http://test/a')
#            b = URIRef('http://test/b')
#            c = URIRef('http://test/c')
#            d = URIRef('http://test/d')
#            for s,p,o in g.triples((None,implies,None)):
#                formulaA = s
#                formulaB = o
#
#                #contexts test
#                assert len(list(g.contexts()))==3
#
#                #contexts (with triple) test
#                assert len(list(g.contexts((a,d,c))))==2
#
#                #triples test cases
#                assert type(list(g.triples(
#                        (None,RDF.type,RDFS.Class)))[0][0]) == BNode
#                assert len(list(g.triples((None,implies,None))))==1
#                assert len(list(g.triples((None,RDF.type,None))))==3
#                assert len(list(g.triples((None,RDF.type,None),formulaA)))==1
#                assert len(list(g.triples((None,None,None),formulaA)))==2
#                assert len(list(g.triples((None,None,None),formulaB)))==1
#                assert len(list(g.triples((None,None,None))))==5
#                assert len(list(g.triples(
#                        (None,URIRef('http://test/d'),None),formulaB)))==1
#                assert len(list(g.triples(
#                        (None,URIRef('http://test/d'),None))))==1
#
#                #Remove test cases
#                g.remove((None,implies,None))
#                assert len(list(g.triples((None,implies,None))))==0
#                assert len(list(g.triples((None,None,None),formulaA)))==2
#                assert len(list(g.triples((None,None,None),formulaB)))==1
#                g.remove((None,b,None),formulaA)
#                assert len(list(g.triples((None,None,None),formulaA)))==1
#                g.remove((None,RDF.type,None),formulaA)
#                assert len(list(g.triples((None,None,None),formulaA)))==0
#                g.remove((None,RDF.type,RDFS.Class))
#
#                #remove_context tests
#                formulaBContext=Context(g,formulaB)
#                g.remove_context(formulaB)
#                assert len(list(g.triples((None,RDF.type,None))))==2
#                assert len(g)==3 assert len(formulaBContext)==0
#                g.remove((None,None,None))
#                assert len(g)==0
