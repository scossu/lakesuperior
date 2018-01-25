import pytest

from shutil import rmtree

from rdflib import URIRef

from lakesuperior.store_layouts.ldp_rs.lmdb_store import LmdbStore

@pytest.fixture(scope='class')
def store():
    store = LmdbStore('/tmp/test_lmdbstore')
    yield store
    store.close()
    rmtree('/tmp/test_lmdbstore')


@pytest.mark.usefixtures('store')
class TestLmdbStore:
    '''
    Unit tests for LMDB store.
    '''
    def test_create_triple(self, store):
        '''
        Test creation of a single triple.
        '''
        store.begin()
        store.add((
            URIRef('urn:test:s'), URIRef('urn:test:p'), URIRef('urn:test:o')))
        store.commit()

        res1 = set(store.triples((None, None, None)))
        res2 = set(store.triples((
            URIRef('urn:test:s'), URIRef('urn:test:p'), URIRef('urn:test:o'))))
        assert len(res1) == 1
        assert len(res2) == 1
        assert (
            URIRef('urn:test:s'), URIRef('urn:test:p'),
            URIRef('urn:test:o')) in res1 & res2


    def test_triple_match_1bound(self, store):
        '''
        Test triple patterns matching one bound term.
        '''
        res1 = set(store.triples((URIRef('urn:test:s'), None, None)))
        res2 = set(store.triples((None, URIRef('urn:test:p'), None)))
        res3 = set(store.triples((None, None, URIRef('urn:test:o'))))
        assert res1 == {(
            URIRef('urn:test:s'), URIRef('urn:test:p'), URIRef('urn:test:o'))}
        assert res2 == res1
        assert res3 == res2


    def test_triple_match_2bound(self, store):
        '''
        Test triple patterns matching two bound terms.
        '''
        res1 = set(store.triples(
            (URIRef('urn:test:s'), URIRef('urn:test:p'), None)))
        res2 = set(store.triples(
            (URIRef('urn:test:s'), None, URIRef('urn:test:o'))))
        res3 = set(store.triples(
            (None, URIRef('urn:test:p'), URIRef('urn:test:o'))))
        assert res1 == {(
            URIRef('urn:test:s'), URIRef('urn:test:p'), URIRef('urn:test:o'))}
        assert res2 == res1
        assert res3 == res2


    def test_triple_no_match(self, store):
        '''
        Test various mismatches.
        '''
        store.begin()
        store.add((
            URIRef('urn:test:s'),
            URIRef('urn:test:p2'), URIRef('urn:test:o2')))
        store.add((
            URIRef('urn:test:s3'),
            URIRef('urn:test:p3'), URIRef('urn:test:o3')))
        store.commit()
        res1 = set(store.triples((None, None, None)))
        assert len(res1) == 3

        res1 = set(store.triples(
            (URIRef('urn:test:s2'), URIRef('urn:test:p'), None)))
        res2 = set(store.triples(
            (URIRef('urn:test:s3'), None, URIRef('urn:test:o'))))
        res3 = set(store.triples(
            (None, URIRef('urn:test:p3'), URIRef('urn:test:o2'))))

        assert len(res1) == len(res2) == len(res3) == 0


    def test_remove(self, store):
        '''
        Test removing one or more triples.
        '''
        store.begin()
        store.remove((URIRef('urn:test:s3'),
                URIRef('urn:test:p3'), URIRef('urn:test:o3')))
        store.commit()

        res1 = set(store.triples((None, None, None)))
        assert len(res1) == 2

        store.begin()
        store.remove((URIRef('urn:test:s'), None, None))
        store.commit()
        res2 = set(store.triples((None, None, None)))
        assert len(res2) == 0


