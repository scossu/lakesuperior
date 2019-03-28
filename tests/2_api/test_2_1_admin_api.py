import pdb
import pytest

from io import BytesIO
from uuid import uuid4

from rdflib import URIRef

from lakesuperior import env
from lakesuperior.api import resource as rsrc_api
from lakesuperior.api import admin as admin_api
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import ChecksumValidationError
from lakesuperior.model.rdf.graph import Graph, from_rdf


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
        with env.app_globals.rdf_store.txn_ctx():
            gr = from_rdf(
                store=env.app_globals.rdf_store,
                data=f'<> <http://ex.org/ns#p1> <info:fcres{uid1}> .',
                format='turtle', publicID=nsc['fcres'][uid2]
            )
        rsrc_api.create_or_replace(uid1, graph=gr)

        assert admin_api.integrity_check() == set()


    def test_check_refint_corrupt(self):
        """
        Corrupt the data store and verify that the missing triple is detected.
        """
        brk_uid = '/test_refint1'
        brk_uri = nsc['fcres'][brk_uid]
        store = env.app_globals.rdf_store
        with store.txn_ctx(True):
            store.remove((URIRef('info:fcres/test_refint1'), None, None))

        check_res = admin_api.integrity_check()

        assert check_res != set()
        assert len(check_res) == 4

        check_trp = {trp[0] for trp in check_res}
        assert {trp[2] for trp in check_trp} == {brk_uri}
        assert (nsc['fcres']['/'], nsc['ldp'].contains, brk_uri) in check_trp
        assert (
                nsc['fcres']['/test_refint2'],
                URIRef('http://ex.org/ns#p1'), brk_uri) in check_trp


    def test_fixity_check_ok(self):
        """
        Verify that fixity check passes for a non-corrupted resource.
        """
        content = BytesIO(uuid4().bytes)
        uid = f'/{uuid4()}'

        rsrc_api.create_or_replace(uid, stream=content)
        admin_api.fixity_check(uid)


    def test_fixity_check_corrupt(self):
        """
        Verify that fixity check fails for a corrupted resource.
        """
        content = BytesIO(uuid4().bytes)
        uid = f'/{uuid4()}'

        _, rsrc = rsrc_api.create_or_replace(uid, stream=content)

        with env.app_globals.rdf_store.txn_ctx():
            with open(rsrc.local_path, 'wb') as fh:
                fh.write(uuid4().bytes)

        with pytest.raises(ChecksumValidationError):
            admin_api.fixity_check(uid)


