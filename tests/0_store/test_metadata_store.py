import pytest

from hashlib import sha256

from lakesuperior import env

mds = env.app_globals.md_store

class TestMetadataStore:
    """
    Tests for the LMDB Metadata store.
    """
    def test_put_checksum(self):
        """
        Put and retrieve a new checksum.
        """
        uri = 'info:fcres/test_checksum'
        cksum = sha256(b'Bogus content')
        with mds.txn_ctx(True):
            mds.put(uri.encode('utf-8'), cksum.digest(), 'checksums')

        with mds.txn_ctx():
            assert mds.get_data(
                    uri.encode('utf-8'), 'checksums') == cksum.digest()


    def test_exception(self):
        """
        Test exceptions within transaction contexts.
        """
        class CustomError(Exception):
            pass

        with pytest.raises(CustomError):
            with mds.txn_ctx():
                raise CustomError()

        with pytest.raises(CustomError):
            with mds.txn_ctx(True):
                raise CustomError()
