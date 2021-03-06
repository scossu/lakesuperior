import pdb
import pytest

from shutil import rmtree

from rdflib import Graph, Namespace, URIRef

from lakesuperior.model.rdf.graph import Graph
from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore


@pytest.fixture(scope='class')
def store():
    """
    Test LMDB store.

    This store has a different life cycle than the one used for tests in higher
    levels of the stack and is not bootstrapped (i.e. starts completely empty).
    """
    env_path = '/tmp/test_lmdbstore'
    # Remove previous test DBs
    rmtree(env_path, ignore_errors=True)
    store = LmdbStore(env_path)
    yield store
    store.close()
    store.destroy()


@pytest.fixture(scope='class')
def trp():
    return (
        (URIRef('urn:s:0'), URIRef('urn:p:0'), URIRef('urn:o:0')),
        # Exact same as [0].
        (URIRef('urn:s:0'), URIRef('urn:p:0'), URIRef('urn:o:0')),
        # NOTE: s and o are in reversed order.
        (URIRef('urn:o:0'), URIRef('urn:p:0'), URIRef('urn:s:0')),
        (URIRef('urn:s:0'), URIRef('urn:p:1'), URIRef('urn:o:0')),
        (URIRef('urn:s:0'), URIRef('urn:p:1'), URIRef('urn:o:1')),
        (URIRef('urn:s:1'), URIRef('urn:p:1'), URIRef('urn:o:1')),
        (URIRef('urn:s:1'), URIRef('urn:p:2'), URIRef('urn:o:2')),
    )

@pytest.mark.usefixtures('trp')
@pytest.mark.usefixtures('store')
class TestGraphInit:
    """
    Test initialization of graphs with different base data sets.
    """
    def test_empty(self, store):
        """
        Test creation of an empty graph.
        """
        # No transaction needed to init an empty graph.
        gr = Graph(store)

        # len() should not need a DB transaction open.
        assert len(gr) == 0


    def test_init_triples(self, trp, store):
        """
        Test creation using a Python set.
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            assert len(gr) == 6

            for t in trp:
                assert t in gr


@pytest.mark.usefixtures('trp')
@pytest.mark.usefixtures('store')
class TestGraphLookup:
    """
    Test triple lookup.
    """

    def test_lookup_all_unbound(self, trp, store):
        """
        Test lookup ? ? ? (all unbound)
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((None, None, None))

            assert len(flt_gr) == 6

            assert trp[0] in flt_gr
            assert trp[2] in flt_gr
            assert trp[3] in flt_gr
            assert trp[4] in flt_gr
            assert trp[5] in flt_gr
            assert trp[6] in flt_gr


    def test_lookup_s(self, trp, store):
        """
        Test lookup s ? ?
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((URIRef('urn:s:0'), None, None))

            assert len(flt_gr) == 3

            assert trp[0] in flt_gr
            assert trp[3] in flt_gr
            assert trp[4] in flt_gr

            assert trp[2] not in flt_gr
            assert trp[5] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup((URIRef('urn:s:8'), None, None))

            assert len(empty_flt_gr) == 0


    def test_lookup_p(self, trp, store):
        """
        Test lookup ? p ?
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((None, URIRef('urn:p:0'), None))

            assert len(flt_gr) == 2

            assert trp[0] in flt_gr
            assert trp[2] in flt_gr

            assert trp[3] not in flt_gr
            assert trp[4] not in flt_gr
            assert trp[5] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup((None, URIRef('urn:p:8'), None))

            assert len(empty_flt_gr) == 0


    def test_lookup_o(self, trp, store):
        """
        Test lookup ? ? o
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((None, None, URIRef('urn:o:1')))

            assert len(flt_gr) == 2

            assert trp[4] in flt_gr
            assert trp[5] in flt_gr

            assert trp[0] not in flt_gr
            assert trp[2] not in flt_gr
            assert trp[3] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup((None, None, URIRef('urn:o:8')))

            assert len(empty_flt_gr) == 0


    def test_lookup_sp(self, trp, store):
        """
        Test lookup s p ?
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((URIRef('urn:s:0'), URIRef('urn:p:1'), None))

            assert len(flt_gr) == 2

            assert trp[3] in flt_gr
            assert trp[4] in flt_gr

            assert trp[0] not in flt_gr
            assert trp[2] not in flt_gr
            assert trp[5] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup((URIRef('urn:s:0'), URIRef('urn:p:2'), None))

            assert len(empty_flt_gr) == 0


    def test_lookup_so(self, trp, store):
        """
        Test lookup s ? o
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((URIRef('urn:s:0'), None, URIRef('urn:o:0')))

            assert len(flt_gr) == 2

            assert trp[0] in flt_gr
            assert trp[3] in flt_gr

            assert trp[2] not in flt_gr
            assert trp[4] not in flt_gr
            assert trp[5] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup((URIRef('urn:s:0'), None, URIRef('urn:o:2')))

            assert len(empty_flt_gr) == 0


    def test_lookup_po(self, trp, store):
        """
        Test lookup ? p o
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup((None, URIRef('urn:p:1'), URIRef('urn:o:1')))

            assert len(flt_gr) == 2

            assert trp[4] in flt_gr
            assert trp[5] in flt_gr

            assert trp[0] not in flt_gr
            assert trp[2] not in flt_gr
            assert trp[3] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup((None, URIRef('urn:p:1'), URIRef('urn:o:2')))

            assert len(empty_flt_gr) == 0


    def test_lookup_spo(self, trp, store):
        """
        Test lookup s p o
        """
        with store.txn_ctx():
            gr = Graph(store, data=set(trp))

            flt_gr = gr.lookup(
                (URIRef('urn:s:1'), URIRef('urn:p:1'), URIRef('urn:o:1'))
            )

            assert len(flt_gr) == 1

            assert trp[5] in flt_gr

            assert trp[0] not in flt_gr
            assert trp[2] not in flt_gr
            assert trp[3] not in flt_gr
            assert trp[4] not in flt_gr
            assert trp[6] not in flt_gr

            # Test for empty results.
            empty_flt_gr = gr.lookup(
                (URIRef('urn:s:1'), URIRef('urn:p:1'), URIRef('urn:o:2'))
            )

            assert len(empty_flt_gr) == 0


@pytest.mark.usefixtures('trp')
@pytest.mark.usefixtures('store')
class TestGraphSlicing:
    """
    Test triple lookup.
    """
    # TODO
    pass



@pytest.mark.usefixtures('trp')
@pytest.mark.usefixtures('store')
class TestGraphOps:
    """
    Test various graph operations.
    """
    def test_len(self, trp, store):
        """
        Test the length of a graph with and without duplicates.
        """
        with store.txn_ctx():
            gr = Graph(store)
            assert len(gr) == 0

            gr.add((trp[0],))
            assert len(gr) == 1

            gr.add((trp[1],)) # Same values
            assert len(gr) == 1

            gr.add((trp[2],))
            assert len(gr) == 2

            gr.add(trp)
            assert len(gr) == 6


    def test_dup(self, trp, store):
        """
        Test operations with duplicate triples.
        """
        with store.txn_ctx():
            gr = Graph(store)

            gr.add((trp[0],))
            assert trp[1] in gr
            assert trp[2] not in gr


    def test_remove(self, trp, store):
        """
        Test adding and removing triples.
        """
        with store.txn_ctx():
            gr = Graph(store)

            gr.add(trp)
            gr.remove(trp[0])
            assert len(gr) == 5
            assert trp[0] not in gr
            assert trp[1] not in gr

            # This is the duplicate triple.
            gr.remove(trp[1])
            assert len(gr) == 5

            # This is the triple in reverse order.
            gr.remove(trp[2])
            assert len(gr) == 4

            gr.remove(trp[4])
            assert len(gr) == 3


    def test_union(self, trp, store):
        """
        Test graph union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:3]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr3 = gr1 | gr2

            assert len(gr3) == 5
            assert trp[0] in gr3
            assert trp[4] in gr3


    def test_ip_union(self, trp, store):
        """
        Test graph in-place union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:3]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr1 |= gr2

            assert len(gr1) == 5
            assert trp[0] in gr1
            assert trp[4] in gr1


    def test_addition(self, trp, store):
        """
        Test graph addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:3]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr3 = gr1 + gr2

            assert len(gr3) == 5
            assert trp[0] in gr3
            assert trp[4] in gr3


    def test_ip_addition(self, trp, store):
        """
        Test graph in-place addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:3]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr1 += gr2

            assert len(gr1) == 5
            assert trp[0] in gr1
            assert trp[4] in gr1


    def test_subtraction(self, trp, store):
        """
        Test graph addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:4]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr3 = gr1 - gr2

            assert len(gr3) == 1
            assert trp[0] in gr3
            assert trp[1] in gr3
            assert trp[2] not in gr3
            assert trp[3] not in gr3
            assert trp[4] not in gr3

            gr3 = gr2 - gr1

            assert len(gr3) == 2
            assert trp[0] not in gr3
            assert trp[1] not in gr3
            assert trp[2] not in gr3
            assert trp[3] not in gr3
            assert trp[4] in gr3
            assert trp[5] in gr3


    def test_ip_subtraction(self, trp, store):
        """
        Test graph in-place addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:4]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr1 -= gr2

            assert len(gr1) == 1
            assert trp[0] in gr1
            assert trp[1] in gr1
            assert trp[2] not in gr1
            assert trp[3] not in gr1
            assert trp[4] not in gr1



    def test_intersect(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:4]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr3 = gr1 & gr2

            assert len(gr3) == 2
            assert trp[2] in gr3
            assert trp[3] in gr3
            assert trp[0] not in gr3
            assert trp[5] not in gr3


    def test_ip_intersect(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:4]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr1 &= gr2

            assert len(gr1) == 2
            assert trp[2] in gr1
            assert trp[3] in gr1
            assert trp[0] not in gr1
            assert trp[5] not in gr1


    def test_xor(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:4]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr3 = gr1 ^ gr2

            assert len(gr3) == 3
            assert trp[2] not in gr3
            assert trp[3] not in gr3
            assert trp[0] in gr3
            assert trp[5] in gr3


    def test_ip_xor(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:4]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr1 ^= gr2

            assert len(gr1) == 3
            assert trp[2] not in gr1
            assert trp[3] not in gr1
            assert trp[0] in gr1
            assert trp[5] in gr1



@pytest.mark.usefixtures('trp')
@pytest.mark.usefixtures('store')
class TestNamedGraphOps:
    """
    Test various operations on a named graph.
    """
    def test_len(self, trp, store):
        """
        Test the length of a graph with and without duplicates.
        """
        imr = Graph(store, uri='http://example.edu/imr01')
        assert len(imr) == 0

        with store.txn_ctx():
            imr.add((trp[0],))
            assert len(imr) == 1

            imr.add((trp[1],)) # Same values
            assert len(imr) == 1

            imr.add((trp[2],))
            assert len(imr) == 2

            imr.add(trp)
            assert len(imr) == 6


    def test_dup(self, trp, store):
        """
        Test operations with duplicate triples.
        """
        imr = Graph(store, uri='http://example.edu/imr01')

        with store.txn_ctx():
            imr.add((trp[0],))
            assert trp[1] in imr
            assert trp[2] not in imr


    def test_remove(self, trp, store):
        """
        Test adding and removing triples.
        """
        with store.txn_ctx():
            imr = Graph(store, uri='http://example.edu/imr01', data={*trp})

            imr.remove(trp[0])
            assert len(imr) == 5
            assert trp[0] not in imr
            assert trp[1] not in imr

            # This is the duplicate triple.
            imr.remove(trp[1])
            assert len(imr) == 5

            # This is the triple in reverse order.
            imr.remove(trp[2])
            assert len(imr) == 4

            imr.remove(trp[4])
            assert len(imr) == 3


    def test_union(self, trp, store):
        """
        Test graph union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:3]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr3 = gr1 | gr2

            assert len(gr3) == 5
            assert trp[0] in gr3
            assert trp[4] in gr3

            assert gr3.uri == None


    def test_ip_union(self, trp, store):
        """
        Test graph in-place union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:3]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr1 |= gr2

            assert len(gr1) == 5
            assert trp[0] in gr1
            assert trp[4] in gr1

            assert gr1.uri == URIRef('http://example.edu/imr01')


    def test_addition(self, trp, store):
        """
        Test graph addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:3]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr3 = gr1 + gr2

            assert len(gr3) == 5
            assert trp[0] in gr3
            assert trp[4] in gr3

            assert gr3.uri == None


    def test_ip_addition(self, trp, store):
        """
        Test graph in-place addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:3]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr1 += gr2

            assert len(gr1) == 5
            assert trp[0] in gr1
            assert trp[4] in gr1

            assert gr1.uri == URIRef('http://example.edu/imr01')


    def test_subtraction(self, trp, store):
        """
        Test graph addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:4]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr3 = gr1 - gr2

            assert len(gr3) == 1
            assert trp[0] in gr3
            assert trp[1] in gr3
            assert trp[2] not in gr3
            assert trp[3] not in gr3
            assert trp[4] not in gr3

            assert gr3.uri == None

            gr3 = gr2 - gr1

            assert len(gr3) == 2
            assert trp[0] not in gr3
            assert trp[1] not in gr3
            assert trp[2] not in gr3
            assert trp[3] not in gr3
            assert trp[4] in gr3
            assert trp[5] in gr3

            assert gr3.uri == None


    def test_ip_subtraction(self, trp, store):
        """
        Test graph in-place addition.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:4]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr1 -= gr2

            assert len(gr1) == 1
            assert trp[0] in gr1
            assert trp[1] in gr1
            assert trp[2] not in gr1
            assert trp[3] not in gr1
            assert trp[4] not in gr1

            assert gr1.uri == URIRef('http://example.edu/imr01')



    def test_intersect(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:4]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr3 = gr1 & gr2

            assert len(gr3) == 2
            assert trp[2] in gr3
            assert trp[3] in gr3
            assert trp[0] not in gr3
            assert trp[5] not in gr3

            assert gr3.uri == None


    def test_ip_intersect(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:4]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr1 &= gr2

            assert len(gr1) == 2
            assert trp[2] in gr1
            assert trp[3] in gr1
            assert trp[0] not in gr1
            assert trp[5] not in gr1

            assert gr1.uri == URIRef('http://example.edu/imr01')


    def test_xor(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:4]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr3 = gr1 ^ gr2

            assert len(gr3) == 3
            assert trp[2] not in gr3
            assert trp[3] not in gr3
            assert trp[0] in gr3
            assert trp[5] in gr3

            assert gr3.uri == None


    def test_ip_xor(self, trp, store):
        """
        Test graph intersextion.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:4]})
            gr2 = Graph(store, uri='http://example.edu/imr02', data={*trp[2:6]})

            gr1 ^= gr2

            assert len(gr1) == 3
            assert trp[2] not in gr1
            assert trp[3] not in gr1
            assert trp[0] in gr1
            assert trp[5] in gr1

            assert gr1.uri == URIRef('http://example.edu/imr01')


@pytest.mark.usefixtures('trp')
@pytest.mark.usefixtures('store')
class TestHybridOps:
    """
    Test operations between IMR and graph.
    """
    def test_hybrid_union(self, trp, store):
        """
        Test hybrid IMR + graph union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:3]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr3 = gr1 | gr2

            assert len(gr3) == 5
            assert trp[0] in gr3
            assert trp[4] in gr3

            assert isinstance(gr3, Graph)
            assert gr3.uri == None

            gr4 = gr2 | gr1

            assert isinstance(gr4, Graph)

            assert gr3 == gr4


    def test_ip_union_imr(self, trp, store):
        """
        Test IMR + graph in-place union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, uri='http://example.edu/imr01', data={*trp[:3]})
            gr2 = Graph(store, data={*trp[2:6]})

            gr1 |= gr2

            assert len(gr1) == 5
            assert trp[0] in gr1
            assert trp[4] in gr1

            assert gr1.uri == URIRef('http://example.edu/imr01')


    def test_ip_union_gr(self, trp, store):
        """
        Test graph + IMR in-place union.
        """
        with store.txn_ctx():
            gr1 = Graph(store, data={*trp[:3]})
            gr2 = Graph(store, uri='http://example.edu/imr01', data={*trp[2:6]})

            gr1 |= gr2

            assert len(gr1) == 5
            assert trp[0] in gr1
            assert trp[4] in gr1

            assert isinstance(gr1, Graph)
