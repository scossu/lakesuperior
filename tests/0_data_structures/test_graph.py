import pytest

from rdflib import Graph, Namespace, URIRef

from lakesuperior.model.graph.graph import SimpleGraph, Imr

@pytest.fixture(scope='class')
def trp():
    return (
        (URIRef('urn:s:0'), URIRef('urn:p:0'), URIRef('urn:o:0')),
        (URIRef('urn:s:0'), URIRef('urn:p:0'), URIRef('urn:o:0')),
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
        assert len(gr) == 5


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
        gr.remove(trp[1])
        assert len(gr) == 4
        assert trp[0] not in gr
        assert trp[1] not in gr

