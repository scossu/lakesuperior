import hashlib

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from os import makedirs, path

import lmdb

from lakesuperior import env


class BaseLmdbStore(metaclass=ABCMeta):
    """
    Generic LMDB store abstract class.

    This class contains convenience method to create an LMDB store for any
    purpose and provides some convenience methods to wrap cursors and
    transactions into contexts.

    This interface can be subclassed for specific storage back ends. It is
    *not* used for :py:class:`~lakesuperior.store.ldp_rs.lmdb_store.LmdbStore`
    which has a more complex lifecycle and setup.
    """

    path = None
    """
    Filesystem path where the database environment is stored.

    This is a mandatory value for implementations.

    :rtype: str
    """

    db_labels = None
    """
    List of databases in the DB environment by label.

    If the environment has only one database, do not override this value (i.e.
    leave it to ``None``).

    :rtype: tuple(str)
    """


    options = {}
    """
    LMDB environment option overrides. Setting this is not required.

    See `LMDB documentation
    <http://lmdb.readthedocs.io/en/release/#environment-class`_ for details
    on available options.

    Default values are available for the following options:

    - ``map_size``: 1 Gib
    - ``max_dbs``: dependent on the number of DBs defined in
      :py:meth:``db_labels``. Only override if necessary.
    - ``max_spare_txns``: dependent on the number of threads, if accessed via
      WSGI, or ``1`` otherwise. Only override if necessary.

    :rtype: dict
    """

    def __init__(self, create=True):
        """
        Initialize DB environment and databases.
        """
        if not path.exists(self.path) and create is True:
            try:
                makedirs(self.path)
            except Exception as e:
                raise IOError(
                    'Could not create the database at {}. Error: {}'.format(
                        self.path, e))

        options = self.options

        if not options.get('max_dbs'):
            options['max_dbs'] = len(self.db_labels)

        if options.get('max_spare_txns', False):
            options['max_spare_txns'] = (
                    env.wsgi_options['workers']
                    if getattr(env, 'wsgi_options', False)
                    else 1)
            logger.info('Max LMDB readers: {}'.format(
                    options['max_spare_txns']))

        self._dbenv = lmdb.open(self.path, **options)

        if self.db_labels is not None:
            self._dbs = {
                label: self._dbenv.open_db(
                    label.encode('ascii'), create=create)
                for label in self.db_labels}


    @property
    def dbenv(self):
        """
        LMDB environment handler.

        :rtype: :py:class:`lmdb.Environment`
        """
        return self._dbenv


    @property
    def dbs(self):
        """
        List of databases in the environment, as LMDB handles.

        These handles can be used to begin transactions.

        :rtype: tuple
        """
        return self._dbs


    @contextmanager
    def txn(self, write=False):
        """
        Transaction context manager.

        :param bool write: Whether a write transaction is to be opened.

        :rtype: lmdb.Transaction
        """
        try:
            txn = self.dbenv.begin(write=write)
            yield txn
            txn.commit()
        except:
            txn.abort()
            raise
        finally:
            txn = None


    @contextmanager
    def cur(self, index=None, txn=None, write=False):
        """
        Handle a cursor on a database by its index as a context manager.

        An existing transaction can be used, otherwise a new one will be
        automatically opened and closed within the cursor context.

        :param str index: The database index. If not specified, a cursor is
            opened for the main database environment.
        :param lmdb.Transaction txn: Existing transaction to use. If not
            specified, a new transaction will be opened.
        :param bool write: Whether a write transaction is to be opened. Only
            meaningful if ``txn`` is ``None``.

        :rtype: lmdb.Cursor
        """
        db = None if index is None else self.dbs[index]

        if txn is None:
            with self.txn(write=write) as _txn:
                cur = _txn.cursor(db)
                yield cur
                cur.close()
        else:
            try:
                cur = txn.cursor(db)
                yield cur
            finally:
                if cur:
                    cur.close()
                    cur = None
