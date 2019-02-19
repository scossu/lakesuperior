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


    def test_addition(self, trp):
        """
        Test graph addition.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr3 = gr1 + gr2

        assert len(gr3) == 5
        assert trp[0] in gr3
        assert trp[4] in gr3


    def test_ip_addition(self, trp):
        """
        Test graph in-place addition.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr1 += gr2

        assert len(gr1) == 5
        assert trp[0] in gr1
        assert trp[4] in gr1


    def test_subtraction(self, trp):
        """
        Test graph addition.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

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


    def test_ip_subtraction(self, trp):
        """
        Test graph in-place addition.
        """
        gr1 = SimpleGraph()
        gr2 = SimpleGraph()

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr1 -= gr2

        assert len(gr1) == 1
        assert trp[0] in gr1
        assert trp[1] in gr1
        assert trp[2] not in gr1
        assert trp[3] not in gr1
        assert trp[4] not in gr1



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



@pytest.mark.usefixtures('trp')
class TestImrOps:
    """
    Test various graph operations.
    """
    def test_len(self, trp):
        """
        Test the length of a graph with and without duplicates.
        """
        imr = Imr(uri='http://example.edu/imr01')
        assert len(imr) == 0

        imr.add((trp[0],))
        assert len(imr) == 1

        imr.add((trp[1],)) # Same values
        assert len(imr) == 1

        imr.add((trp[2],))
        assert len(imr) == 2

        imr.add(trp)
        assert len(imr) == 6


    def test_dup(self, trp):
        """
        Test operations with duplicate triples.
        """
        imr = Imr(uri='http://example.edu/imr01')
        #import pdb; pdb.set_trace()

        imr.add((trp[0],))
        assert trp[1] in imr
        assert trp[2] not in imr


    def test_remove(self, trp):
        """
        Test adding and removing triples.
        """
        imr = Imr(uri='http://example.edu/imr01')

        imr.add(trp)
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


    def test_union(self, trp):
        """
        Test graph union.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr3 = gr1 | gr2

        assert len(gr3) == 5
        assert trp[0] in gr3
        assert trp[4] in gr3

        assert gr3.uri == 'http://example.edu/imr01'


    def test_ip_union(self, trp):
        """
        Test graph in-place union.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr1 |= gr2

        assert len(gr1) == 5
        assert trp[0] in gr1
        assert trp[4] in gr1

        assert gr1.uri == 'http://example.edu/imr01'


    def test_addition(self, trp):
        """
        Test graph addition.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr3 = gr1 + gr2

        assert len(gr3) == 5
        assert trp[0] in gr3
        assert trp[4] in gr3

        assert gr3.uri == 'http://example.edu/imr01'


    def test_ip_addition(self, trp):
        """
        Test graph in-place addition.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:3])
        gr2.add(trp[2:6])

        gr1 += gr2

        assert len(gr1) == 5
        assert trp[0] in gr1
        assert trp[4] in gr1

        assert gr1.uri == 'http://example.edu/imr01'


    def test_subtraction(self, trp):
        """
        Test graph addition.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

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

        assert gr3.uri == 'http://example.edu/imr01'


    def test_ip_subtraction(self, trp):
        """
        Test graph in-place addition.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr1 -= gr2

        assert len(gr1) == 1
        assert trp[0] in gr1
        assert trp[1] in gr1
        assert trp[2] not in gr1
        assert trp[3] not in gr1
        assert trp[4] not in gr1

        assert gr1.uri == 'http://example.edu/imr01'



    def test_intersect(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr3 = gr1 & gr2

        assert len(gr3) == 2
        assert trp[2] in gr3
        assert trp[3] in gr3
        assert trp[0] not in gr3
        assert trp[5] not in gr3

        assert gr3.uri == 'http://example.edu/imr01'


    def test_ip_intersect(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr1 &= gr2

        assert len(gr1) == 2
        assert trp[2] in gr1
        assert trp[3] in gr1
        assert trp[0] not in gr1
        assert trp[5] not in gr1

        assert gr1.uri == 'http://example.edu/imr01'


    def test_xor(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr3 = gr1 ^ gr2

        assert len(gr3) == 3
        assert trp[2] not in gr3
        assert trp[3] not in gr3
        assert trp[0] in gr3
        assert trp[5] in gr3

        assert gr3.uri == 'http://example.edu/imr01'


    def test_ip_xor(self, trp):
        """
        Test graph intersextion.
        """
        gr1 = Imr(uri='http://example.edu/imr01')
        gr2 = Imr(uri='http://example.edu/imr02')

        gr1.add(trp[0:4])
        gr2.add(trp[2:6])

        gr1 ^= gr2

        assert len(gr1) == 3
        assert trp[2] not in gr1
        assert trp[3] not in gr1
        assert trp[0] in gr1
        assert trp[5] in gr1

        assert gr1.uri == 'http://example.edu/imr01'


