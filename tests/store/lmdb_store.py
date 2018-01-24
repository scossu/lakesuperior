import pytest

from rdflib import URIRef

from lakesuperior.store_layouts.ldp_rs.lmdb_store import LmdbStore

@pytest.fixture(scope='module')
def store():
    return LmdbStore('/tmp/lmdbstore')


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

        res = set(store.triples((None, None, None)))
        assert len(res) == 1
        assert (URIRef('urn:test:s'), URIRef('urn:test:p'), URIRef('urn:test:o')) \
                in res
