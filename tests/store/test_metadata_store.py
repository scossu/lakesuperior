import pytest

from hashlib import sha256

from lakesuperior.store.ldp_rs.metadata_store import MetadataStore


class TestMetadataStore:
    """
    Tests for the LMDB Metadata store.
    """
    def test_put_checksum(self):
        """
        Put a new checksum.
        """
        uri = 'info:fcres/test_checksum'
        cksum = sha256(b'Bogus content')
        mds = MetadataStore()
        with mds.cur(index='checksums', write=True) as cur:
            cur.put(uri.encode('utf-8'), cksum.digest())

        with mds.cur(index='checksums') as cur:
            assert cur.get(uri.encode('utf-8')) == cksum.digest()


    def test_separate_txn(self):
        """
        Open a transaction and put a new checksum.

        Same as test_put_checksum but wrapping the cursor in a separate
        transaction. This is really to test the base store which is an abstract
        class.
        """
        uri = 'info:fcres/test_checksum_separate'
        cksum = sha256(b'More bogus content.')
        mds = MetadataStore()
        with mds.txn(True) as txn:
            with mds.cur(index='checksums', txn=txn) as cur:
                cur.put(uri.encode('utf-8'), cksum.digest())

        with mds.txn() as txn:
            with mds.cur(index='checksums', txn=txn) as cur:
                assert cur.get(uri.encode('utf-8')) == cksum.digest()


    def test_exception(self):
        """
        Test exceptions within cursor and transaction contexts.
        """
        uri = 'info:fcres/test_checksum_exception'
        cksum = sha256(b'More bogus content.')
        mds = MetadataStore()

        class CustomError(Exception):
            pass

        with pytest.raises(CustomError):
            with mds.txn() as txn:
                raise CustomError()

        with pytest.raises(CustomError):
            with mds.txn() as txn:
                with mds.cur(index='checksums', txn=txn) as cur:
                    raise CustomError()

        with pytest.raises(CustomError):
            with mds.cur(index='checksums') as cur:
                raise CustomError()

        with pytest.raises(CustomError):
            with mds.txn(write=True) as txn:
                raise CustomError()

        with pytest.raises(CustomError):
            with mds.txn(write=True) as txn:
                with mds.cur(index='checksums', txn=txn) as cur:
                    raise CustomError()

        with pytest.raises(CustomError):
            with mds.cur(index='checksums', write=True) as cur:
                raise CustomError()
