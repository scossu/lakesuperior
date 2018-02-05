import hashlib
import logging

from contextlib import ContextDecorator, ExitStack
from multiprocessing import Process
from os import makedirs
from os.path import exists, abspath
from threading import Lock, Thread
from urllib.request import pathname2url

import lmdb

from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib import Graph, Namespace, URIRef


logger = logging.getLogger(__name__)


def s2b(u, enc='UTF-8'):
    '''
    Convert a string into a bytes object.
    '''
    return u.encode(enc)


def b2s(u, enc='UTF-8'):
    '''
    Convert a bytes or memoryview object into a string.
    '''
    return bytes(u).decode(enc)


class NoTxnError(Exception):
    '''
    Raised if a store operation is attempted while no transaction is present.
    '''
    def __str__(self):
        return 'No transaction active in the store.'


class TxnManager(ContextDecorator):
    '''
    Handle ACID transactions with an LmdbStore.

    Wrap this within a `with` statement:

    >>> with TxnManager(store, True):
    ...     # Do something with the database
    >>>

    The transaction will be opened and handled automatically.
    '''
    def __init__(self, store, write=False):
        '''
        Begin and close a transaction in a store.

        @param store (LmdbStore) The store to open a transaction on.
        @param write (bool) Whether the transaction is read-write. Default is
        False (read-only transaction).
        '''
        self.store = store
        self.write = write

    def __enter__(self):
        self.store.begin(write=self.write)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.store.rollback()
            # If the tx fails, leave the index queue alone. There may still be
            # jobs left from other requests.
        else:
            self.store.commit()
            if len(self.store._data_queue):
                self.store._apply_changes()
            if len(self.store._idx_queue):
                # Synchronous.
                self.store._run_indexing()
                # Threading.
                #job = Thread(target=self.store._run_indexing)
                # Multiprocess.
                #job = Process(target=self.store._run_indexing)
                #job.start()
                #logger.info('Started indexing job #{}'.format(job.ident))


class LmdbStore(Store):
    '''
    LMDB-backed store.

    This store class uses two LMDB environments (i.e. two files): one for the
    critical (preservation-worthy) data and the other for the index data which
    can be rebuilt from the main database. @TODO For now, data and indices are
    in the same environment due to complications in handling transaction
    contexts.

    There are 3 main data sets (preservation worthy data):

    - tk:t (triple key: pickled triple; unique keys)
    - tk:c (Triple key: pickled context; multi-valued keys)
    - pfx:ns (prefix: pickled namespace; unique)

    And 8 indices to optimize lookup for all possible bound/unbound term
    combination in a triple:

    - c:tk (pickled context URI: triple key)
    - sk:tk (subject key: triple key)
    - pk:tk (pred key: triple key)
    - ok:tk (object key: triple key)
    - spk:tk (subject + predicate key: triple key)
    - sok:tk (subject + object key: triple key)
    - pok:tk (predicate + object key: triple key)
    - ns:pfx (pickled namespace: prefix; unique)

    The above indices (except for ns:pfx) are all multi-valued and store
    fixed-length hash values referring to triples for economy's sake.

    The search keys for terms are hashed on lookup. @TODO If this is too slow,
    we may want to index term hashes.
    '''
    context_aware = True
    # This is a hassle to maintain for no apparent gain. If some use is devised
    # in the future, it may be revised.
    formula_aware = False
    graph_aware = True
    transaction_aware = True

    '''
    LMDB map size. See http://lmdb.readthedocs.io/en/release/#environment-class
    '''
    MAP_SIZE = 1024 ** 4 # 1Tb

    '''
    Key hashing algorithm. If you are paranoid, use SHA1. Otherwise, MD5 is
    faster and takes up less space (16 bytes vs. 20 bytes). This may make a
    visible difference because keys are generated and parsed very often.
    '''
    KEY_HASH_ALGO = 'sha1'

    '''
    Whether the default graph is the union graph. At the moment only False
    is supported.
    '''
    DEFAULT_UNION = False

    DEFAULT_GRAPH_URI = URIRef('urn:fcrepo:default_graph')

    data_keys = ('tk:c', 'tk:t', 'pfx:ns')
    idx_keys = (
            'c:tk', 'sk:tk', 'pk:tk', 'ok:tk', 'spk:tk', 'sok:tk', 'pok:tk',
            'ns:pfx')

    data_env = None
    idx_env = None
    db = None
    dbs = {}
    data_txn = None
    idx_txn = None
    is_txn_rw = None

    '''
    List of actions to be performed when a transaction is committed.

    Each element is a tuple of (action name, database index, key, value).
    '''
    _data_queue = []
    '''
    Set of indices to update. A set has been preferred to a list since the
    index update don't need to be sequential and there may be duplicate entries
    that can be eliminated.

    Each element is a tuple of (triple key, pickled context, pre-pickled triple
    ). The third value can be None, and in that case, it is calculated from
    the triple key.
    '''
    _idx_queue = set()


    def __init__(self, path, identifier=None):
        self.__open = False

        self.identifier = identifier or URIRef(pathname2url(abspath(path)))
        super(LmdbStore, self).__init__(path)

        self._pickle = self.node_pickler.dumps
        self._unpickle = self.node_pickler.loads


    def __len__(self, context=None):
        '''
        Return length of the dataset.
        '''
        if context == self or context is None:
            context = Graph(identifier=self.DEFAULT_GRAPH_URI)

        if context.identifier is not self.DEFAULT_GRAPH_URI:
            #dataset = self.triples((None, None, None), context)
            dataset = (tk for tk in self.curs['c:tk'].iternext_dup())
            return len(set(dataset))
        else:
            return self.data_txn.stat(self.dbs['tk:t'])['entries']


    @property
    def is_open(self):
        return self.__open


    def open(self, path, create=True):
        '''
        Open the database.

        The database is best left open for the lifespan of the server. Read
        transactions can be opened as needed. Write transaction should be
        opened and closed within a single HTTP request to ensure atomicity of
        the request.

        This method is called outside of the main transaction. All cursors
        are created separately within the transaction.
        '''
        self._init_db_environments(path, create)
        if self.data_env == NO_STORE:
            return NO_STORE
        self.__open = True

        return VALID_STORE


    def begin(self, write=False):
        '''
        Begin the main write transaction and create cursors.
        '''
        if not self.is_open:
            raise RuntimeError('Store must be opened first.')
        logger.info('Beginning a {} transaction.'.format(
            'read/write' if write else 'read-only'))
        self.data_txn = self.data_env.begin(buffers=True)
        self.idx_txn = self.idx_env.begin(buffers=True)
        self.is_txn_rw = write
        # Cursors.
        self.curs = self.get_data_cursors(self.data_txn)
        self.curs.update(self.get_idx_cursors(self.idx_txn))


    @property
    def is_txn_open(self):
        '''
        Whether the main transaction is open.
        '''
        try:
            self.data_txn.id()
            self.idx_txn.id()
        except (lmdb.Error, AttributeError) as e:
            #logger.info('Main transaction does not exist or is closed.')
            return False
        else:
            #logger.info('Main transaction is open.')
            return True


    def cur(self, index):
        '''
        Return a new cursor by its index.
        '''
        if index in self.idx_keys:
            txn = self.idx_txn
            src = self.idx_keys
        elif index in self.data_keys:
            txn = self.data_txn
            src = self.data_keys
        else:
            return ValueError('Cursor key not found.')

        return txn.cursor(self.dbs[index])


    def get_data_cursors(self, txn):
        '''
        Build the main data cursors for a transaction.

        @param txn (lmdb.Transaction) This can be a read or write transaction.

        @return dict(string, lmdb.Cursor) Keys are index labels, values are
        index cursors.
        '''
        return {
            'tk:t': txn.cursor(self.dbs['tk:t']),
            'tk:c': txn.cursor(self.dbs['tk:c']),
            'pfx:ns': txn.cursor(self.dbs['pfx:ns']),
        }


    def get_idx_cursors(self, txn):
        '''
        Build the index cursors for a transaction.

        @param txn (lmdb.Transaction) This can be a read or write transaction.

        @return dict(string, lmdb.Cursor) Keys are index labels, values are
        index cursors.
        '''
        cur = {}
        for key in self.idx_keys:
            cur[key] = txn.cursor(self.dbs[key])

        return cur


    def close(self, commit_pending_transaction=False):
        '''
        Close the database connection.

        Do this at server shutdown.
        '''
        self.__open = False
        if self.is_txn_open:
            if commit_pending_transaction:
                self.commit()
            else:
                self.rollback()

        self.data_env.close()


    def destroy(self, path):
        '''
        Destroy the store.

        https://www.youtube.com/watch?v=lIVq7FJnPwg

        @param path (string) Path of the folder containing the database(s).
        '''
        if exists(path):
            rmtree(path)


    def add(self, triple, context=None, quoted=False):
        '''
        Add a triple and start indexing.

        @param triple (tuple:rdflib.Identifier) Tuple of three identifiers.
        @param context (rdflib.Identifier | None) Context identifier.
        'None' inserts in the default graph.
        @param quoted (bool) Not used.
        '''
        assert context != self, "Cannot add triple directly to store"
        Store.add(self, triple, context)

        #logger.info('Adding triple: {}'.format(triple))
        if self.DEFAULT_UNION:
            raise NotImplementedError()
            # @TODO
        elif context is None:
            context = self.DEFAULT_GRAPH_URI
        pk_trp = self._pickle(triple)
        trp_key = hashlib.new(self.KEY_HASH_ALGO, pk_trp).digest()

        needs_indexing = False
        with self.cur('tk:t') as cur:
            if not cur.set_key(trp_key):
                self._enqueue_action('put', 'tk:t', trp_key, pk_trp)
                needs_indexing = True

        pk_ctx = self._pickle(context.identifier) \
                if isinstance(context, Graph) \
                else self._pickle(context)
        with self.cur('tk:c') as cur:
            if not cur.set_key_dup(trp_key, pk_ctx):
                self._enqueue_action('put', 'tk:c', trp_key, pk_ctx)
                needs_indexing = True

        if needs_indexing:
            self._idx_queue.add((trp_key, pk_ctx, triple))


    def remove(self, triple_pattern, context=None):
        '''
        Remove a triple and start indexing.
        '''
        if self.DEFAULT_UNION:
            raise NotImplementedError()
            # @TODO
        elif context is None:
            context = self.DEFAULT_GRAPH_URI

        #import pdb; pdb.set_trace()
        pk_ctx = self._pickle(context.identifier) \
                if isinstance(context, Graph) \
                else self._pickle(context)
        for trp_key in self._triple_keys(triple_pattern, context):
            # Delete context association.
            with self.cur('tk:c') as cur:
                if cur.set_key_dup(trp_key, pk_ctx):
                    triple = self._key_to_triple(trp_key)
                    self._enqueue_action('delete', 'tk:c', trp_key, pk_ctx)

                    # If no other contexts are associated with the triple,
                    # delete it.
                    with self.cur('tk:t') as trp_cur:
                        if not cur.set_key(trp_key):
                            self._enqueue_action(
                                    'delete', 'tk:c', trp_key, None)

                    self._idx_queue.add((trp_key, pk_ctx, triple))


    def triples(self, triple_pattern, context=None):
        '''
        Generator over matching triples.

        @param triple_pattern (tuple) 3 RDFLib terms
        @param context (rdflib.Graph | None) Context graph, if available.
        If a graph is given, only its identifier is stored.
        '''
        for tk in self._triple_keys(triple_pattern, context):
            yield self._key_to_triple(tk), context


    def bind(self, prefix, namespace):
        '''
        Bind a prefix to a namespace.
        '''
        prefix = s2b(prefix)
        namespace = s2b(namespace)
        with self.data_txn.cursor(self.dbs['pfx:ns']) as cur:
            cur.put(prefix, namespace)
        with self.idx_txn.cursor(self.dbs['ns:pfx']) as cur:
            cur.put(namespace, prefix)


    def namespace(self, prefix):
        '''
        Get the namespace for a prefix.
        '''
        with self.cur('pfx:ns') as cur:
            ns = cur.get(s2b(prefix))
            return Namespace(b2s(ns)) if ns is not None else None


    def prefix(self, namespace):
        '''
        Get the prefix associated with a namespace.

        @NOTE A namespace can be only bound to one prefix in this
        implementation.
        '''
        with self.cur('ns:pfx') as cur:
            prefix = cur.get(s2b(namespace))
            return b2s(prefix) if prefix is not None else None


    def namespaces(self):
        '''
        Get an iterator of all prefix: namespace bindings.
        '''
        with self.cur('pfx:ns') as cur:
            bindings = iter(cur)
            return ((b2s(pfx), Namespace(b2s(ns))) for pfx, ns in bindings)


    def contexts(self, triple=None):
        '''
        Get a list of all contexts.

        @return generator:URIRef
        '''
        if triple:
            with self.cur('tk:c') as cur:
                cur.set_key(self._to_key(triple))
                contexts = cur.iternext_dup()
        else:
            with self.cur('c:tk') as cur:
                contexts = cur.iternext_nodup()

        return (self._unpickle(ctx) for ctx in contexts)


    def add_graph(self, graph):
        '''
        Add a graph to the database.

        This creates an empty graph by associating the graph URI with the
        pickled `None` value. This prevents from removing the graph when all
        triples are removed.

        This may be called by supposedly read-only operations:
        https://github.com/RDFLib/rdflib/blob/master/rdflib/graph.py#L1623
        Therefore it needs to open a write transaction. This is not ideal
        but the only way to handle datasets in RDFLib.

        @param graph (URIRef) URI of the named graph to add.
        '''
        pk_none = self._pickle(None)
        pk_ctx = self._pickle(graph)
        with self.data_env.begin(write=True).cursor(self.dbs['tk:c']) \
                as tk2c_cur:
            tk2c_cur.put(pk_none, pk_ctx)

        with self.idx_env.begin(write=True)\
                .cursor(self.dbs['c:tk']) as c2tk_cur:
            c2tk_cur.put(pk_ctx, pk_none)


    def remove_graph(self, graph):
        '''
        Remove all triples from graph and the graph itself.

        @param graph (URIRef) URI of the named graph to remove.
        '''
        self.remove((None, None, None), graph)

        pk_none = self._pickle(None)
        pk_ctx = self._pickle(graph)
        self._enqueue_action('delete', 'tk:c', pk_none, pk_ctx)
        self._idx_queue.add((None, pk_ctx, None))

        with self.cur('c:tk') as cur:
            if cur.set_key_dup(self._pickle(graph), self._pickle(None)):
                self.curs['tk:c'].delete()


    def commit(self):
        '''
        Commit main transaction and push action queue.
        '''
        if self.is_txn_open:
            self.data_txn.commit()
            self.idx_txn.commit()
        self.data_txn = self.idx_txn = self.is_txn_rw = None


    def rollback(self):
        '''
        Roll back main transaction.
        '''
        if self.is_txn_open:
            self.data_txn.abort()
            self.idx_txn.abort()
        self.data_txn = self.idx_txn = self.is_txn_rw = None


    #def _next_lex_key(self, db=None):
    #    '''
    #    Calculate the next closest byte sequence in lexicographical order.

    #    This is needed to fill the next available slot after the last one in
    #    LMDB. Keys are byte strings. This is convenient to keep key
    #    lengths as small as possible because they are referenced in several
    #    indices.
    #    '''
    #    with self.env.begin(buffers=True) as txn:
    #        with txn.cursor(db) as cur:
    #            has_entries = cur.last()
    #            if has_entries:
    #                next = bytearray(cur.key())
    #            else:
    #                # First key in db.
    #                return b'\x00'
    #    try:
    #        next[-1] += 1
    #    # If the value exceeds 256, i.e. the current value is the last one,
    #    # append a new \x00 and the next iteration will start incrementing that
    #    except ValueError:
    #        next.append(0)

    #    return next


    ## PRIVATE METHODS ##

    def _triple_keys(self, triple_pattern, context=None):
        '''
        Generator over matching triple keys.

        This method is used by `triples` which returns native Python tuples,
        as well as by other methods that need to iterate and filter triple
        keys without incurring in the overhead of converting them to triples.

        @param triple_pattern (tuple) 3 RDFLib terms
        @param context (rdflib.Graph | None) Context graph, if available.
        If a graph is given, only its identifier is stored.
        '''
        if context == self:
            context = None

        if self.DEFAULT_UNION:
            raise NotImplementedError()
            # In theory, this is what should happen:
            #if context == self.DEFAULT_GRAPH_URI
            #    # Any pattern with unbound context
            #    for tk in self._lookup(triple_pattern, tkey):
            #        yield self._key_to_triple(tk)
            #    return
        elif context is None:
            context = self.DEFAULT_GRAPH_URI

        tkey = self._to_key(triple_pattern)

        # Shortcuts
        pk_ctx = self._pickle(context.identifier) \
                if isinstance(context, Graph) \
                else self._pickle(context)
        if not self.curs['c:tk'].set_key(pk_ctx):
            # Context not found.
            return iter(())

        # s p o c
        if all(triple_pattern):
            if self.curs['tk:c'].set_key_dup(tkey, pk_ctx):
                yield tkey
                return
            else:
                # Triple not found.
                return iter(())

        # ? ? ? c
        elif not any(triple_pattern):
            # Get all triples from the context
            for tk in self.curs['c:tk'].iternext_dup():
                yield tk

        # Regular lookup.
        else:
            for tk in self._lookup(triple_pattern, tkey):
                if self.curs['c:tk'].set_key_dup(pk_ctx, tk):
                    yield tk
            return


    def _init_db_environments(self, path, create=True):
        '''
        Initialize the DB environment.

        The main database is kept in one file, the indices in a separate one
        (these may be even further split up depending on performance
        considerations).

        @param path The base path to contain the databases.
        @param create (bool) If True, the environment and its databases are
        created.
        '''
        if not exists(path):
            if create is True:
                makedirs(path)
            else:
                return NO_STORE
        self.data_env = lmdb.open(path + '/main', subdir=False, create=create,
                map_size=self.MAP_SIZE, max_dbs=4, readahead=False)
        self.idx_env = lmdb.open(path + '/index', subdir=False, create=create,
                map_size=self.MAP_SIZE, max_dbs=10, readahead=False)

        # Open and optionally create main databases.
        self.dbs = {
            # Main databases.
            'tk:t': self.data_env.open_db(b'tk:t', create=create),
            'tk:c': self.data_env.open_db(b'tk:c', create=create, dupsort=True),
            'pfx:ns': self.data_env.open_db(b'pfx:ns', create=create),
            # Index.
            'ns:pfx': self.idx_env.open_db(b'ns:pfx', create=create),
        }
        # Other index databases.
        for db_key in self.idx_keys:
            self.dbs[db_key] = self.idx_env.open_db(s2b(db_key),
                    dupsort=True, dupfixed=True, create=create)


    def _to_key(self, obj):
        '''
        Convert a triple, quad or term into a key.

        The key is the checksum of the pickled object, therefore unique for
        that object. The hashing algorithm is specified in `KEY_HASH_ALGO`.

        @param obj (Object) Anything that can be pickled. Pairs of terms, as
        well as triples and quads, are expressed as tuples within the scope of
        this application.

        @return bytes
        '''
        return hashlib.new(self.KEY_HASH_ALGO, self._pickle(obj)).digest()


    def _key_to_triple(self, key):
        '''
        Look up for the hash key of a triple and return the triple as a tuple.

        @param key (bytes) Hash key of triple.

        @return Tuple with triple elements or None if key is not found.
        '''
        pk_trp = self.curs['tk:t'].get(key)

        return self._unpickle(pk_trp) if pk_trp else None


    def _lookup(self, triple_pattern, tkey=None):
        '''
        Look up triples based on a triple pattern.

        @return iterator of matching triple keys.
        '''
        s, p, o = triple_pattern

        if s is not None:
            if p is not None:
                # s p o
                if o is not None:
                    if self.curs['tk:t'].set_key(tkey):
                        yield tkey
                        return
                    else:
                        return iter(())
                # s p ?
                else:
                    cur = self.curs['spk:tk']
                    term = self._pickle((s, p))
            else:
                # s ? o
                if o is not None:
                    cur = self.curs['sok:tk']
                    term = self._pickle((s, o))
                # s ? ?
                else:
                    cur = self.curs['sk:tk']
                    term = self._pickle(s)
        else:
            if p is not None:
                # ? p o
                if o is not None:
                    cur = self.curs['pok:tk']
                    term = self._pickle((p, o))
                # ? p ?
                else:
                    cur = self.curs['pk:tk']
                    term = self._pickle(p)
            else:
                # ? ? o
                if o is not None:
                    cur = self.curs['ok:tk']
                    term = self._pickle(o)
                # ? ? ?
                else:
                    # Get all triples in the database
                    for c in self.curs['tk:t'].iternext(values=False):
                        yield c
                    return

        key = hashlib.new(self.KEY_HASH_ALGO, term).digest()
        if cur.set_key(key):
            for match in cur.iternext_dup():
                yield match
        else:
            return iter(())


    def _enqueue_action(self, action, db, k, v):
        '''
        Enqueue an action to be performed in a write transaction.

        Actions are accumulated sequentially and then executed once the
        `_run_update` method is called. This is usually done by the
        TxnManager class.

        @param action (string) One of 'put', 'putmulti' or 'delete'.
        @param db (string) Label of the database to perform the action.
        @param k (bytes) Key to update.
        @param v (bytes) Value to insert or delete.
        '''
        if not action in ('put', 'putmulti', 'delete'):
            raise NameError('No action with name {}.'.format(action))

        self._data_queue.append((action, db, k, v))


    def _apply_changes(self):
        '''
        Apply changes in `_data_queue`.
        '''
        with ExitStack() as stack:
            data_txn = stack.enter_context(
                    self.data_env.begin(write=True, buffers=True))
            logger.info('Beginning data insert. Data write lock acquired.')

            curs = {
                task[1]: stack.enter_context(
                        data_txn.cursor(self.dbs[task[1]]))
                for task in self._data_queue
            }
            while len(self._data_queue):
                action, db, k, v = self._data_queue.pop()
                if action == 'put':
                    curs[db].put(k, v)
                elif action == 'putmulti':
                    # With 'putmulti', `k` is a series of 2-tuples and `v` is
                    # ignored.
                    data = k
                    curs[db].putmulti(data)
                elif action == 'delete':
                    if v is None:
                        # Delete all values for the key.
                        if curs[db].set_key(k):
                            curs[db].delete(dupdata=True)
                    else:
                        # Delete only a specific k:v pair.
                        if curs[db].set_key_dup(k, v):
                            curs[db].delete(dupdata=False)
                else:
                    raise ValueError(
                        'Action type \'{}\' is not supported.' .format(action))
        logger.info('Data insert completed. Data write lock released.')


    def _run_indexing(self):
        '''
        Update indices for a given triple.

        If the triple is found, add indices. if it is not found, delete them.
        This method is run asynchronously and may outlive the HTTP request.

        @param key (bytes) Unique key associated with the triple.
        @param pk_ctx (bytes) Pickled context term.
        @param triple (tuple: rdflib.Identifier) Tuple of 3 RDFLib terms.
        This can be provided if already pre-calculated, otherwise it will be
        retrieved from the store using `trp_key`.
        '''
        with ExitStack() as stack:
            data_txn = stack.enter_context(self.data_env.begin(buffers=True))
            idx_txn = stack.enter_context(
                    self.idx_env.begin(write=True, buffers=True))
            logger.info('Index started. Index write lock acquired.')
            data_curs = self.get_data_cursors(data_txn)
            idx_curs = self.get_idx_cursors(idx_txn)

            lock = Lock()
            while len(self._idx_queue):
                lock.acquire()
                trp_key, pk_ctx, triple = self._idx_queue.pop()

                if trp_key is None and triple is None:
                    # This is when a graph is deleted.
                    if not data_curs['tk:c'].set_key(pk_ctx):
                        pk_none = self._pickle(None)
                        if idx_curs['c:tk'].set_key_dup(pk_none, pk_ctx):
                            idx_curs['c:tk'].delete()
                    lock.release()
                    continue

                if triple is None:
                    triple = self._key_to_triple(trp_key)

                s, p, o = triple
                term_keys = {
                    'sk:tk': self._to_key(s),
                    'pk:tk': self._to_key(p),
                    'ok:tk': self._to_key(o),
                    'spk:tk': self._to_key((s, p)),
                    'sok:tk': self._to_key((s, o)),
                    'pok:tk': self._to_key((p, o)),
                }

                if data_curs['tk:t'].get(trp_key):
                    # Add to index.
                    for ikey in term_keys:
                        idx_curs[ikey].put(term_keys[ikey], trp_key)
                else:
                    # Delete from index if a match is found.
                    for ikey in term_keys:
                        if idx_curs[ikey].set_key_dup(
                                term_keys[ikey], trp_key):
                            idx_curs[ikey].delete()

                # Add or remove context association index.
                if data_curs['tk:c'].set_key_dup(trp_key, pk_ctx):
                    idx_curs['c:tk'].put(pk_ctx, trp_key)
                elif idx_curs['c:tk'].set_key_dup(pk_ctx, trp_key):
                    idx_curs['c:tk'].delete()
                lock.release()

        logger.info('Index completed. Index write lock released.')


    ## Convenience methods—not necessary for functioning but useful for
    ## debugging.

    def _keys_in_ctx(self, pk_ctx):
        '''
        Convenience method to list all keys in a context.

        @param pk_ctx (bytes) Pickled context URI.

        @return Iterator:tuple Generator of triples.
        '''
        cur = self.curs['c:tk']
        if cur.set_key(pk_ctx):
            tkeys = cur.iternext_dup()
            return {self._key_to_triple(tk) for tk in tkeys}
        else:
            return set()


    def _ctx_for_key(self, tkey):
        '''
        Convenience method to list all contexts that a key is in.

        @param tkey (bytes) Triple key.

        @return Iterator:URIRef Generator of context URIs.
        '''
        cur = self.curs['tk:c']
        if cur.set_key(tkey):
            ctx = cur.iternext_dup()
            return {self._unpickle(c) for c in ctx}
        else:
            return set()
