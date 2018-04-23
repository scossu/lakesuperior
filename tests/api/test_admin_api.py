import pdb
import pytest

from rdflib import Graph, URIRef

from lakesuperior import env
from lakesuperior.api import resource as rsrc_api
from lakesuperior.api import admin as admin_api
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager


@pytest.mark.usefixtures('db')
class TestAdminApi:
    """
    Test admin operations.
    """

    def test_check_refint_ok(self):
        """
        Check that referential integrity is OK.
        """
        uid1 = '/test_refint1'
        uid2 = '/test_refint2'
        gr = Graph().parse(
                data='<> <http://ex.org/ns#p1> <info:fcres{}> .'.format(uid1),
                format='turtle', publicID=nsc['fcres'][uid2])
        rsrc_api.create_or_replace(uid1, graph=gr)

        assert admin_api.integrity_check() == set()


    def test_check_refint_corrupt(self):
        """
        Corrupt the data store and verify that the missing triple is detected.
        """
        brk_uid = '/test_refint1'
        brk_uri = nsc['fcres'][brk_uid]
        store = env.app_globals.rdf_store
        with TxnManager(store, True):
            store.remove((URIRef('info:fcres/test_refint1'), None, None))

        #import pdb; pdb.set_trace()
        check_res = admin_api.integrity_check()

        assert check_res != set()
        assert len(check_res) == 4

        check_trp = {trp[0] for trp in check_res}
        assert {trp[2] for trp in check_trp} == {brk_uri}
        assert (nsc['fcres']['/'], nsc['ldp'].contains, brk_uri) in check_trp
        assert (
                nsc['fcres']['/test_refint2'], 
                URIRef('http://ex.org/ns#p1'), brk_uri) in check_trp


