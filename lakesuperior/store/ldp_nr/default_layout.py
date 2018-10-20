import hashlib
import logging
import os
import shutil

from uuid import uuid4

from lakesuperior import env
from lakesuperior.store.ldp_nr.base_non_rdf_layout import BaseNonRdfLayout
from lakesuperior.exceptions import ChecksumValidationError


logger = logging.getLogger(__name__)


class DefaultLayout(BaseNonRdfLayout):
    """
    Default file layout.

    This is a simple filesystem layout that stores binaries in pairtree folders
    in a local filesystem. Parameters can be specified for the
    """
    @staticmethod
    def local_path(root, uuid, bl=4, bc=4):
        """
        Generate the resource path splitting the resource checksum according to
        configuration parameters.

        :param str uuid: The resource UUID. This corresponds to the content
            checksum.
        """
        logger.debug('Generating path from uuid: {}'.format(uuid))
        term = len(uuid) if bc == 0 else min(bc * bl, len(uuid))

        path = [uuid[i : i + bl] for i in range(0, term, bl)]

        if bc > 0:
            path.append(uuid[term :])
        path.insert(0, root)

        return '/'.join(path)


    def __init__(self, *args, **kwargs):
        """Set up path segmentation parameters."""
        super().__init__(*args, **kwargs)

        self.bl = self.config['pairtree_branch_length']
        self.bc = self.config['pairtree_branches']


    ## INTERFACE METHODS ##

    def bootstrap(self):
        """Initialize binary file store."""
        try:
            shutil.rmtree(self.root)
        except FileNotFoundError:
            pass
        os.makedirs(self.root + '/tmp')


    def persist(
            self, uid, stream, bufsize=8192, prov_cksum=None,
            prov_cksum_algo=None):
        r"""
        Store the stream in the file system.

        This method handles the file in chunks. for each chunk it writes to a
        temp file and adds to a checksum. Once the whole file is written out
        to disk and hashed, the temp file is moved to its final location which
        is determined by the hash value.

        :param str uid: UID of the resource.
        :param IOstream stream: file-like object to persist.
        :param int bufsize: Chunk size. 2\*\*12 to 2\*\*15 is a good range.
        :param str prov_cksum: Checksum provided by the client to verify
            that the content received matches what has been sent. If None (the
            default) no verification will take place.
        :param str prov_cksum_algo: Verification algorithm to validate the
            integrity of the user-provided data. If this is different from
            the default hash algorithm set in the application configuration,
            which is used to calclate the checksum of the file for storing
            purposes, a separate hash is calculated specifically for validation
            purposes. Clearly it's more efficient to use the same algorithm and
            avoid a second checksum calculation.
        """
        # The temp file is created on the destination filesystem to minimize
        # time and risk of moving it to its final destination.
        tmp_fname = f'{self.root}/tmp/{uuid4()}'

        default_hash_algo = \
                env.app_globals.config['application']['uuid']['algo']
        if prov_cksum_algo is None:
            prov_cksum_algo = default_hash_algo
        try:
            with open(tmp_fname, 'wb') as f:
                logger.debug(f'Writing temp file to {tmp_fname}.')

                store_hash = hashlib.new(default_hash_algo)
                verify_hash = (
                        store_hash if prov_cksum_algo == default_hash_algo
                        else hashlib.new(prov_cksum_algo))
                size = 0
                while True:
                    buf = stream.read(bufsize)
                    if not buf:
                        break
                    store_hash.update(buf)
                    if verify_hash is not store_hash:
                        verify_hash.update(buf)
                    f.write(buf)
                    size += len(buf)

                if prov_cksum and verify_hash.hexdigest() != prov_cksum:
                    raise ChecksumValidationError(
                        uid, prov_cksum, verify_hash.hexdigest())
        except:
            logger.exception(f'File write failed on {tmp_fname}.')
            os.unlink(tmp_fname)
            raise
        if size == 0:
            logger.warn('Zero-length file received.')

        # If the file exists already, don't bother rewriting it.
        dst = __class__.local_path(
                self.root, store_hash.hexdigest(), self.bl, self.bc)
        if os.path.exists(dst):
            logger.info(f'File exists on {dst}. Not overwriting.')

        # Move temp file to final destination.
        logger.debug(f'Saving file to disk: {dst}')
        if not os.access(os.path.dirname(dst), os.X_OK):
            os.makedirs(os.path.dirname(dst))
        os.rename(tmp_fname, dst)

        return store_hash.hexdigest(), size


    def delete(self, uuid):
        """See BaseNonRdfLayout.delete."""
        os.unlink(__class__.local_path(self.root, uuid, self.bl, self.bc))
