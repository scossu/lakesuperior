import logging
import os
import shutil

from hashlib import sha1
from uuid import uuid4

from lakesuperior.store.ldp_nr.base_non_rdf_layout import BaseNonRdfLayout


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


    def persist(self, stream, bufsize=8192):
        r"""
        Store the stream in the file system.

        This method handles the file in chunks. for each chunk it writes to a
        temp file and adds to a checksum. Once the whole file is written out
        to disk and hashed, the temp file is moved to its final location which
        is determined by the hash value.

        :param IOstream stream: file-like object to persist.
        :param int bufsize: Chunk size. 2\*\*12 to 2\*\*15 is a good range.
        """
        tmp_file = '{}/tmp/{}'.format(self.root, uuid4())
        try:
            with open(tmp_file, 'wb') as f:
                logger.debug('Writing temp file to {}.'.format(tmp_file))

                hash = sha1()
                size = 0
                while True:
                    buf = stream.read(bufsize)
                    if not buf:
                        break
                    hash.update(buf)
                    f.write(buf)
                    size += len(buf)
        except:
            logger.exception('File write failed on {}.'.format(tmp_file))
            os.unlink(tmp_file)
            raise
        if size == 0:
            logger.warn('Zero-length file received.')

        # Move temp file to final destination.
        uuid = hash.hexdigest()
        dst = __class__.local_path(self.root, uuid, self.bl, self.bc)
        logger.debug('Saving file to disk: {}'.format(dst))
        if not os.access(os.path.dirname(dst), os.X_OK):
            os.makedirs(os.path.dirname(dst))

        # If the file exists already, don't bother rewriting it.
        if os.path.exists(dst):
            logger.info(
                    'File exists on {}. Not overwriting.'.format(dst))
            os.unlink(tmp_file)
        else:
            os.rename(tmp_file, dst)

        return uuid, size


    def delete(self, uuid):
        """See BaseNonRdfLayout.delete."""
        os.unlink(__class__.local_path(self.root, uuid, self.bl, self.bc))
