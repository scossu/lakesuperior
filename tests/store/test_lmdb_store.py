import pytest

from os import path
from shutil import rmtree

from rdflib import Graph, Namespace, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI
from rdflib.namespace import RDF, RDFS

from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore


@pytest.fixture(scope='class')
def store():
    """
    Test LMDB store.

    This store has a different life cycle than the one used for tests in higher
    levels of the stack.
    """
    env_path = '/tmp/test_lmdbstore'
    # If a previous test segfaulted, a corrupt database may be still around
    rmtree(env_path, ignore_errors=True)
    print(f'Removed store at {env_path}')
    store = LmdbStore(env_path)
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
        with store.txn_ctx():
            assert store.is_txn_open
            assert not store.is_txn_rw
        assert not store.is_txn_open

        with store.txn_ctx(True):
            assert store.is_txn_open
            assert store.is_txn_rw
        assert not store.is_txn_open
        assert not store.is_txn_rw

        try:
            with store.txn_ctx():
                raise RuntimeError()
        except RuntimeError:
            assert not store.is_txn_open


    def test_rollback(self, store):
        '''
        Test rolling back a transaction.
        '''
        try:
            with store.txn_ctx(True):
                store.add((
                    URIRef('urn:nogo:s'), URIRef('urn:nogo:p'),
                    URIRef('urn:nogo:o')))
                raise RuntimeError() # This should roll back the transaction.
        except RuntimeError:
            pass

        with store.txn_ctx():
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
        with store.txn_ctx(True):
            store.add(trp)

        with store.txn_ctx():
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
        with store.txn_ctx():
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
        with store.txn_ctx():
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
        with store.txn_ctx():
            pattern = (
                URIRef('urn:test:s'), URIRef('urn:test:p'),
                URIRef('urn:test:o'))
            res1 = set(store.triples(pattern))
            assert _clean(res1) == {pattern}


    def test_triple_no_match_1bound(self, store):
        '''
        Test empty matches with 1 bound term.
        '''
        with store.txn_ctx(True):
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
        with store.txn_ctx(True):
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
        with store.txn_ctx(True):
            res1 = set(store.triples((
                URIRef('urn:test:s2'), URIRef('urn:test:p3'),
                URIRef('urn:test:o2'))))

            assert len(res1) == 0


    def test_remove(self, store):
        '''
        Test removing one or more triples.
        '''
        with store.txn_ctx(True):
            store.remove((URIRef('urn:test:s3'),
                    URIRef('urn:test:p3'), URIRef('urn:test:o3')))

        with store.txn_ctx():
            res1 = set(store.triples((None, None, None)))
            assert len(res1) == 2

        with store.txn_ctx(True):
            store.remove((URIRef('urn:test:s'), None, None))
            res2 = set(store.triples((None, None, None)))
            assert len(res2) == 0



@pytest.mark.usefixtures('store')
class TestRemoveMulti:
    '''
    Tests for proper removal of multiple combinations of triple and context.
    '''
    @pytest.fixture
    def data(self):
        return {
            'spo1': (
            URIRef('urn:test:s1'), URIRef('urn:test:p1'), URIRef('urn:test:o1')),
            'spo2': (
            URIRef('urn:test:s1'), URIRef('urn:test:p1'), URIRef('urn:test:o2')),
            'spo3': (
            URIRef('urn:test:s1'), URIRef('urn:test:p1'), URIRef('urn:test:o3')),
            'c1': URIRef('urn:test:c1'),
            'c2': URIRef('urn:test:c2'),
            'c3': URIRef('urn:test:c3'),
        }


    def test_init(self, store, data):
        """
        Initialize the store with test data.
        """
        with store.txn_ctx(True):
            store.add(data['spo1'], data['c1'])
            store.add(data['spo2'], data['c1'])
            store.add(data['spo3'], data['c1'])
            store.add(data['spo1'], data['c2'])
            store.add(data['spo2'], data['c2'])
            store.add(data['spo3'], data['c2'])
            store.add(data['spo1'], data['c3'])
            store.add(data['spo2'], data['c3'])
            store.add(data['spo3'], data['c3'])

            assert len(store) == 9
            assert len(set(store.triples((None, None, None)))) == 3
            assert len(set(store.triples((None, None, None), data['c1']))) == 3
            assert len(set(store.triples((None, None, None), data['c2']))) == 3
            assert len(set(store.triples((None, None, None), data['c3']))) == 3


    def test_remove_1ctx(self, store, data):
        """
        Test removing all triples from a context.
        """
        with store.txn_ctx(True):
            store.remove((None, None, None), data['c1'])

            assert len(store) == 6
            assert len(set(store.triples((None, None, None)))) == 3
            assert len(set(store.triples((None, None, None), data['c1']))) == 0
            assert len(set(store.triples((None, None, None), data['c2']))) == 3
            assert len(set(store.triples((None, None, None), data['c3']))) == 3


    def test_remove_1subj(self, store, data):
        """
        Test removing one subject from all contexts.
        """
        with store.txn_ctx(True):
            store.remove((data['spo1'][0], None, None))

            assert len(store) == 0



@pytest.mark.usefixtures('store')
class TestCleanup:
    '''
    Tests for proper cleanup on resource deletion.
    '''
    @pytest.fixture
    def data(self):
        return {
            'spo1': (
            URIRef('urn:test:s1'), URIRef('urn:test:p1'), URIRef('urn:test:o1')),
            'spo2': (
            URIRef('urn:test:s2'), URIRef('urn:test:p2'), URIRef('urn:test:o2')),
            'c1': URIRef('urn:test:c1'),
            'c2': URIRef('urn:test:c2'),
        }

    def _is_empty(self, store):
        stats = store.stats()['db_stats']
        for dblabel in ('spo:c', 'c:spo', 's:po', 'p:so', 'o:sp',):
            if stats[dblabel]['ms_entries'] > 0:
                return False

        return True


    def test_cleanup_spo(self, store, data):
        with store.txn_ctx(True):
            store.add(data['spo1'])
        with store.txn_ctx():
            assert not self._is_empty(store)

        with store.txn_ctx(True):
            store.remove(data['spo1'])
        with store.txn_ctx():
            assert self._is_empty(store)


    def test_cleanup_spoc1(self, store, data):
        with store.txn_ctx(True):
            store.add(data['spo1'], data['c1'])
        with store.txn_ctx():
            assert not self._is_empty(store)

        with store.txn_ctx(True):
            store.remove(data['spo1'], data['c1'])
        with store.txn_ctx():
            assert self._is_empty(store)


    def test_cleanup_spoc2(self, store, data):
        with store.txn_ctx(True):
            store.add(data['spo1'], data['c1'])

        with store.txn_ctx(True):
            store.remove((None, None, None), data['c1'])
        with store.txn_ctx():
            assert self._is_empty(store)


    def test_cleanup_spoc3(self, store, data):
        with store.txn_ctx(True):
            store.add(data['spo1'], data['c1'])
            store.add(data['spo2'], data['c1'])

        with store.txn_ctx(True):
            store.remove((data['spo1'][0], None, None), data['c1'])
        with store.txn_ctx():
            assert not self._is_empty(store)

        with store.txn_ctx(True):
            store.remove((data['spo2'][0], None, None), data['c1'])
        with store.txn_ctx():
            assert self._is_empty(store)


    def test_cleanup_spoc4(self, store, data):
        with store.txn_ctx(True):
            store.add(data['spo1'], data['c1'])
            store.add(data['spo2'], data['c1'])
            store.add(data['spo2'], data['c2'])

        with store.txn_ctx(True):
            store.remove((None, None, None))
        with store.txn_ctx():
            assert self._is_empty(store)



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
        with store.txn_ctx(True):
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
        with store.txn_ctx(True):
            for b in bindings:
                pfx, ns = b
                assert store.namespace(pfx) == ns


    def test_pfx2ns(self, store, bindings):
        '''
        Test namespace to prefix conversion.
        '''
        with store.txn_ctx(True):
            for b in bindings:
                pfx, ns = b
                assert store.prefix(ns) == pfx



@pytest.mark.usefixtures('store')
class TestContext:
    '''
    Tests for context handling.
    '''
    def test_add_empty_graph(self, store):
        '''
        Test creating an empty and a non-empty graph.
        '''
        gr_uri = URIRef('urn:bogus:graph#a')

        with store.txn_ctx(True):
            store.add_graph(gr_uri)
            assert gr_uri in {gr.identifier for gr in store.contexts()}


    def test_add_graph_with_triple(self, store):
        '''
        Test creating an empty and a non-empty graph.
        '''
        trp = (URIRef('urn:test:s123'),
                URIRef('urn:test:p123'), URIRef('urn:test:o123'))
        ctx_uri = URIRef('urn:bogus:graph#b')

        with store.txn_ctx(True):
            store.add(trp, ctx_uri)

        with store.txn_ctx():
            assert ctx_uri in {gr.identifier for gr in store.contexts(trp)}


    def test_empty_context(self, store):
        '''
        Test creating and deleting empty contexts.
        '''
        gr_uri = URIRef('urn:bogus:empty#a')

        with store.txn_ctx(True):
            store.add_graph(gr_uri)
            assert gr_uri in {gr.identifier for gr in store.contexts()}
        with store.txn_ctx(True):
            store.remove_graph(gr_uri)
            assert gr_uri not in {gr.identifier for gr in store.contexts()}


    def test_context_ro_txn(self, store):
        '''
        Test creating a context within a read-only transaction.
        '''
        gr_uri = URIRef('urn:bogus:empty#b')

        with store.txn_ctx():
            store.add_graph(gr_uri)
            a = 5 #bogus stuff for debugger
        # The lookup must happen in a separate transaction. The first
        # transaction opens a separate (non-child) R/W transaction while it is
        # already isolated so even after the RW txn is committed, the RO one
        # doesn't know anything about the changes.
        # If the RW transaction could be nested inside the RO one that would
        # allow a lookup in the same transaction, but this does not seem to be
        # possible.
        with store.txn_ctx():
            assert gr_uri in {gr.identifier for gr in store.contexts()}
        with store.txn_ctx(True):
            store.remove_graph(gr_uri)
            assert gr_uri not in {gr.identifier for gr in store.contexts()}


    def test_add_trp_to_ctx(self, store):
        '''
        Test adding triples to a graph.
        '''
        gr_uri = URIRef('urn:bogus:graph#a') # From previous test
        gr2_uri = URIRef('urn:bogus:graph#z') # Never created before
        trp1 = (URIRef('urn:s:1'), URIRef('urn:p:1'), URIRef('urn:o:1'))
        trp2 = (URIRef('urn:s:2'), URIRef('urn:p:2'), URIRef('urn:o:2'))
        trp3 = (URIRef('urn:s:3'), URIRef('urn:p:3'), URIRef('urn:o:3'))
        trp4 = (URIRef('urn:s:4'), URIRef('urn:p:4'), URIRef('urn:o:4'))

        with store.txn_ctx(True):
            store.add(trp1, gr_uri)
            store.add(trp2, gr_uri)
            store.add(trp2, gr_uri) # Duplicate; dropped.
            store.add(trp2, None) # Goes to the default graph.
            store.add(trp3, gr2_uri)
            store.add(trp3, gr_uri)
            store.add(trp4) # Goes to the default graph.

        # Quick size checks.
        with store.txn_ctx():
            assert len(set(store.triples((None, None, None)))) == 5
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

        # Verify that contexts are in the right place.
        with store.txn_ctx():
            # trp3 is in both graphs.
            res_no_ctx = store.triples(trp3)
            res_ctx = store.triples(trp3, gr2_uri)
            for res in res_no_ctx:
                assert Graph(identifier=gr_uri) in res[1]
                assert Graph(identifier=gr2_uri) in res[1]
            for res in res_ctx:
                assert Graph(identifier=gr_uri) in res[1]
                assert Graph(identifier=gr2_uri) in res[1]


    def test_delete_from_ctx(self, store):
        '''
        Delete triples from a named graph and from the default graph.
        '''
        gr_uri = URIRef('urn:bogus:graph#a')
        gr2_uri = URIRef('urn:bogus:graph#b')

        with store.txn_ctx(True):
            store.remove((None, None, None), gr2_uri)
            assert len(set(store.triples((None, None, None), gr2_uri))) == 0
            assert len(set(store.triples((None, None, None), gr_uri))) == 3

        with store.txn_ctx(True):
            store.remove((URIRef('urn:s:1'), None, None))
            assert len(set(store.triples(
                (URIRef('urn:s:1'), None, None), gr_uri))) == 0
            assert len(set(store.triples((None, None, None), gr_uri))) == 2
            assert len(set(store.triples((None, None, None)))) == 3

        # This should result in no change because the graph does not exist.
        with store.txn_ctx(True):
            store.remove((None, None, None), URIRef('urn:phony:graph#xyz'))
            store.remove(
                    (URIRef('urn:s:1'), None, None),
                    URIRef('urn:phony:graph#xyz'))
            assert len(set(store.triples((None, None, None), gr_uri))) == 2
            assert len(set(store.triples((None, None, None)))) == 3

        with store.txn_ctx(True):
            store.remove((URIRef('urn:s:4'), None, None),
                    RDFLIB_DEFAULT_GRAPH_URI)
            assert len(set(store.triples((None, None, None)))) == 2

        with store.txn_ctx(True):
            store.remove((None, None, None))
            assert len(set(store.triples((None, None, None)))) == 0
            assert len(set(store.triples((None, None, None), gr_uri))) == 0
            assert len(store) == 0


    def test_remove_shared_ctx(self, store):
        """
        Remove a context that shares triples with another one.
        """
        trp1 = (
                URIRef('urn:bogus:shared_s:1'), URIRef('urn:bogus:shared_p:1'),
                URIRef('urn:bogus:shared_o:1'))
        trp2 = (
                URIRef('urn:bogus:shared_s:2'), URIRef('urn:bogus:shared_p:2'),
                URIRef('urn:bogus:shared_o:2'))
        trp3 = (
                URIRef('urn:bogus:shared_s:3'), URIRef('urn:bogus:shared_p:3'),
                URIRef('urn:bogus:shared_o:3'))
        ctx1 = URIRef('urn:bogus:shared_graph#a')
        ctx2 = URIRef('urn:bogus:shared_graph#b')

        with store.txn_ctx(True):
            store.add(trp1, ctx1)
            store.add(trp2, ctx1)
            store.add(trp2, ctx2)
            store.add(trp3, ctx2)

        with store.txn_ctx(True):
            store.remove_graph(ctx1)

        with store.txn_ctx():
            assert len(set(store.triples(trp1))) == 0
            assert len(set(store.triples(trp2))) == 1
            assert len(set(store.triples(trp3))) == 1






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
#        from rdflib import Graph, plugin
#        from rdflib.store import Store
#        store = plugin.get('Lmdb', Store)('/tmp/rdflibtest')
#        return Graph(store).parse('''
#        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
#        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
#        @prefix : <http://test/> .
#        {:a :b :c; a :foo} => {:a :d :c} .
#        _:foo a rdfs:Class .
#        :a :d :c .
#        ''', format='n3')
#
#    def test_basic(self, sample_gr):
#        from rdflib.namespace import RDF
#        with sample_gr.store.txn_ctx():
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
#                assert len(g)==3
#                assert len(formulaBContext)==0
#                g.remove((None,None,None))
#                assert len(g)==0
