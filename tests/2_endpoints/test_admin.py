import pytest

from io import BytesIO
from uuid import uuid4

from lakesuperior.api import resource as rsrc_api


@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestAdminApi:
    """
    Test admin endpoint.
    """

    def test_fixity_check_ok(self):
        """
        Verify that fixity check passes for a non-corrupted resource.
        """
        uid = uuid4()
        content = uuid4().bytes
        path = f'/ldp/{uid}'
        fix_path = f'/admin/{uid}/fixity'

        self.client.put(
            path, data=content, headers={'content-type': 'text/plain'})

        rsp = self.client.get(fix_path)

        assert rsp.status_code == 200
        assert rsp.json['uid'] == f'/{uid}'
        assert rsp.json['pass'] == True


    def test_fixity_check_corrupt(self):
        """
        Verify that fixity check fails for a corrupted resource.
        """
        uid = uuid4()
        content = uuid4().bytes
        path = f'/ldp/{uid}'
        fix_path = f'/admin/{uid}/fixity'

        self.client.put(
            path, data=content, headers={'content-type': 'text/plain'})

        rsrc = rsrc_api.get(f'/{uid}')

        with open(rsrc.local_path, 'wb') as fh:
            fh.write(uuid4().bytes)

        rsp = self.client.get(fix_path)

        assert rsp.status_code == 200
        assert rsp.json['uid'] == f'/{uid}'
        assert rsp.json['pass'] == False


    def test_fixity_check_missing(self):
        """
        Verify that fixity check is not performed on a missing resource.
        """
        uid = uuid4()
        content = uuid4().bytes
        path = f'/ldp/{uid}'
        fix_path = f'/admin/{uid}/fixity'

        assert self.client.get(fix_path).status_code == 404

        self.client.put(
            path, data=content, headers={'content-type': 'text/plain'})

        self.client.delete(path)

        assert self.client.get(fix_path).status_code == 410


