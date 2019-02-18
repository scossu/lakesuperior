import pytest

from rdflib import Graph, Namespace, URIRef

from lakesuperior.model.graph.graph import SimpleGraph, Imr

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
class TestGraphOps:
    """
    Test various graph operations.
    """
    def test_len(self, trp):
        """
        Test the length of a graph with and without duplicates.
        """
        gr = SimpleGraph()
        assert len(gr) == 0

        gr.add((trp[0],))
        assert len(gr) == 1

        gr.add((trp[1],)) # Same values
        assert len(gr) == 1

        gr.add((trp[2],))
        assert len(gr) == 2

        gr.add(trp)
        assert len(gr) == 6


    def test_dup(self, trp):
        """
        Test operations with duplicate triples.
        """
        gr = SimpleGraph()
        #import pdb; pdb.set_trace()

        gr.add((trp[0],))
        assert trp[1] in gr
        assert trp[2] not in gr


    def test_remove(self, trp):
        """
        Test adding and removing triples.
        """
        gr = SimpleGraph()

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


    def test_union(self, trp):
        """
        Test graph union.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr3 = gr1 | gr2

        assert len(gr3) == 5
        assert trp[0] in gr3
        assert trp[4] in gr3


    def test_ip_union(self, trp):
        """
        Test graph in-place union.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr1 |= gr2

        assert len(gr1) == 5
        assert trp[0] in gr1
        assert trp[4] in gr1


    def test_intersect(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr3 = gr1 & gr2

        assert len(gr3) == 2
        assert trp[2] in gr3
        assert trp[3] in gr3
        assert trp[0] not in gr3
        assert trp[5] not in gr3


    def test_ip_intersect(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr1 &= gr2

        assert len(gr1) == 2
        assert trp[2] in gr1
        assert trp[3] in gr1
        assert trp[0] not in gr1
        assert trp[5] not in gr1


    def test_xor(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr3 = gr1 ^ gr2

        assert len(gr3) == 3
        assert trp[2] not in gr3
        assert trp[3] not in gr3
        assert trp[0] in gr3
        assert trp[5] in gr3


    def test_ip_xor(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr1 ^= gr2

        assert len(gr1) == 3
        assert trp[2] not in gr1
        assert trp[3] not in gr1
        assert trp[0] in gr1
        assert trp[5] in gr1


