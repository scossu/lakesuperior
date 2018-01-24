import hashlib
import logging

from os import makedirs
from os.path import exists, abspath

import lmdb

from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib.term import URIRef
from six import b
from six.moves.urllib.request import pathname2url


logger = logging.getLogger(__name__)


def s2b(u):
    return u.encode('utf-8')

def b2s(u):
    return bytes(u).decode('utf-8')


class NoTxnError(Exception):
    '''
    Raised if a store operation is attempted while no transaction is present.
    '''
    def __str__(self):
        return 'No transaction active in the store.'


class LmdbStore(Store):
    '''
    LMDB-backed store.

    This store class uses two LMDB environment (i.e. two files): one for the
    critical (preservation-worthy) data and the other for the index data which
    can be rebuilt from the main database. @TODO For now, data and indices are
    in the same environment due to complications in handling transaction
    contexts.

    There are 2 main data sets (preservation worthy data):

    - tk:t (triple key: pickled triple; unique keys)
    - tk:c (Triple key: pickled context; multi-valued keys)

    And 7 indices to optimize lookup for all possible bound/unbound term
    combination in a triple:

    - c:tk (pickled context URI: triple key)
    - sk:tk (subject key: triple key)
    - pk:tk (pred key: triple key)
    - ok:tk (object key: triple key)
    - spk:tk (subject + predicate key: triple key)
    - sok:tk (subject + object key: triple key)
    - pok:tk (predicate + object key: triple key)

    The above indices are all multi-valued and store fixed-length hash values
    referring to triples for economy's sake.

    The search keys for terms are hashed on lookup. @TODO If this is too slow,
    we may want to index term hashes.
    '''
    context_aware = True
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

    data_keys = ('tk:c', 'tk:t', 'ns:pfx')
    idx_keys = (
            'c:tk', 'sk:tk', 'pk:tk', 'ok:tk', 'spk:tk', 'sok:tk', 'pok:tk',
            'pfx:ns')

    db_env = None
    db = None
    idx_db = {}
    txn = None


    def __init__(self, path, identifier=None):
        self.__open = False
        self.identifier = identifier
        super(LmdbStore, self).__init__(path)

        self._pickle = self.node_pickler.dumps
        self._unpickle = self.node_pickler.loads


    def _init_db_environment(self, path, create=True):
        '''
        Initialize the DB environment.
        If `create` is True, the environment and its databases are created.
        '''
        if not exists(path):
            if create is True:
                makedirs(path)
            else:
                return NO_STORE
        self.db_env = lmdb.open(path, create=create, map_size=self.MAP_SIZE,
                max_dbs=12, readahead=False)

        # Open and optionally create main databases.
        self.trp_db = self.db_env.open_db(b'tk:t', create=create)
        self.ctx_db = self.db_env.open_db(b'tk:c', create=create, dupsort=True)
        self.pfx_db = self.db_env.open_db(b'pfx:ns', create=create)
        # Index databases.
        for db_key in self.idx_keys:
            self.idx_db[db_key] = self.db_env.open_db(s2b(db_key),
                    dupsort=True, dupfixed=True, create=create)
        self.idx_db['ns:pfx'] = self.db_env.open_db(
                b'ns:pfx', create=create)


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
        if self.identifier is None:
            self.identifier = URIRef(pathname2url(abspath(path)))

        self._init_db_environment(path, create)
        if self.db_env == NO_STORE:
            return NO_STORE
        self.__open = True

        return VALID_STORE

    def begin(self):
        '''
        Begin the main write transaction and create cursors.
        '''
        if not self.is_open:
            raise RuntimeError('Store must be opened first.')
        self.txn = self.db_env.begin(write=True, buffers=True)
        # Cursors.
        self.data_cur = self.get_data_cursors(self.txn)
        self.idx_cur = self.get_idx_cursors(self.txn)


    def get_data_cursors(self, txn):
        '''
        Build the main data cursors for a transaction.

        @param txn (lmdb.Transaction) This can be a read or write transaction.

        @return dict(string, lmdb.Cursor) Keys are index labels, values are
        index cursors.
        '''
        return {
            'tk:t': txn.cursor(self.trp_db),
            'tk:c': txn.cursor(self.ctx_db),
            'pfx:ns': txn.cursor(self.pfx_db),
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
            cur[key] = self.txn.cursor(self.idx_db[key])

        return cur


    @property
    def is_txn_open(self):
        '''
        Whether the main write transaction is open.
        '''
        try:
            self.txn.id()
        except lmdb.Error:
            return False
        else:
            return True


    def close(self, commit_pending_transaction=False):
        '''
        Close the database connection.

        Do this at server shutdown.
        '''
        self.__open = False
        if self.is_txn_open:
            if commit_pending_transaction:
                self.tx.commit()
            else:
                self.tx.abort()
            self.tx = None

        self.db_env.close()


    def add(self, triple, context=None):
        '''
        Add a triple and start indexing.

        @param triple (tuple:rdflib.Identifier) Tuple of three identifiers.
        @param context (rdflib.Identifier | None) Context identifier.
        'None' inserts in the default graph.
        '''
        assert self.is_txn_open, "The Store must be open."
        assert context != self, "Can not add triple directly to store"
        Store.add(self, triple, context)

        context = context or self.DEFAULT_GRAPH_URI
        pk_trp = self._pickle(triple)
        trp_key = hashlib.new(self.KEY_HASH_ALGO, pk_trp).digest()
        # If it returns False, the triple had already been added.
        trp_added = self.data_cur['tk:t'].put(trp_key, pk_trp, overwrite=False)

        pk_ctx = self._pickle(context)
        ctx_added = self.data_cur['tk:c'].put(trp_key, pk_ctx, overwrite=False)

        if ctx_added or trp_added:
            # @TODO make await
            self._do_index(triple, trp_key, pk_ctx)


    def remove(self, triple_pattern, context=None):
        '''
        Remove a triple and start indexing.
        '''
        context = context or self.DEFAULT_GRAPH_URI
        pk_ctx = self._pickle(context)
        for trp in self.triples(triple_pattern, context):
            trp_key = self._to_key(trp)
            need_indexing = False

            # Delete context association.
            if self.data_cur['tk:c'].set_key_dup(pk_ctx, trp_key):
                self.data_cur['tk:c'].delete()
                need_indexing = True

                # If no other contexts are associated w/ the triple, delete it.
                if not self.data_cur['tk:c'].set_key(trp_key) and (
                        self.data_cur['tk:t'].set_key(trp_key)):
                    self.data_cur['tk:t'].delete()

                # @TODO make await
                self._do_index(trp, trp_key, pk_ctx)


    # @TODO Make async
    def _do_index(self, triple, trp_key, pk_ctx):
        '''
        Create indices for a given triple.

        If the triple is found, add indices. if it is not found, delete them.

        @param triple (tuple: rdflib.Identifier) Tuple of 3 RDFLib terms.
        @param key (bytes) Unique key associated with the triple.
        @param pk_ctx (bytes) Pickled context term.
        '''
        s, p, o = triple
        term_keys = {
            'sk:tk': self._to_key(s),
            'pk:tk': self._to_key(p),
            'ok:tk': self._to_key(o),
            'spk:tk': self._to_key((s, p)),
            'sok:tk': self._to_key((s, o)),
            'pok:tk': self._to_key((p, o)),
        }

        if self.data_cur['tk:t'].get(trp_key):
            # Add to index.
            for ikey in term_keys:
                self.idx_cur[ikey].put(term_keys[ikey], trp_key)
        else:
            # Delete from index if a match is found.
            for ikey in self.term_keys:
                if self.idx_cur[ikey].set_key_dup(term_keys[ikey], trp_key):
                    self.idx_cur[ikey].delete()

        # Add or remove context association index.
        if self.data_cur['tk:c'].get(trp_key, pk_ctx):
            self.idx_cur['c:tk'].put(pk_ctx, trp_key)
        else:
            if self.idx_cur['c:tk'].set_key_dup(pk_ctx, trp_key):
                self.idx_cur['c:tk'].delete()


    def triples(self, triple_pattern, context=None):
        '''
        Generator over matching triples.
        '''
        assert self.__open, "The Store must be open."
        if context == self:
            context = None

        tkey = self._to_key(triple_pattern)

        with self.db_env.begin(buffers=True) as txn:
            if context is not None:
                pk_ctx = self._pickle(context)
                if not self.idx_cur['c:tk'].set_key(pk_ctx):
                    # Context not found.
                    return iter(())
                # If all triple elements are bound
                if all(triple_pattern):
                    with txn.cursor(self.ctx_db) as cur:
                        if cur.set_key_dup(tkey, pk_ctx):
                            yield self._key_to_triple(tkey)
                        else:
                            # Triple not found.
                            return iter(())
                # If some are unbound
                else:
                    # If some are bound
                    if any(triple_pattern):
                        # Find the lookup index
                        with txn.cursor(self.idx_db['c:tk']) as cur:
                            for tk in self._lookup(triple_pattern):
                                if cur.set_key_dup(pk_ctx, tk):
                                    yield self._key_to_triple(tk)
                    # If all are unbound
                    else:
                        # Get all triples from the context
                        with txn.cursor(self.idx_db['c:tk']) as cur:
                            for tk in cur.iternext_dup():
                                yield self._key_to_triple(tk)
            # If context is unbound
            else:
                # If all triples are bound
                if all(triple_pattern):
                    with txn.cursor(self.trp_db) as cur:
                        match = cur.set_key(tkey)
                        if match:
                            yield self._key_to_triple(match)
                        else:
                            return iter(())
                # If some are unbound
                else:
                    # If some are bound
                    if any(triple_pattern):
                        return self._lookup(triple_pattern)
                    # If all are unbound
                    else:
                        # Get all triples in the database
                        with txn.cursor(self.trp_db) as cur:
                            pk_triples = cur.iternext(keys=False)
                            for pk_trp in pk_triples:
                                yield self._unpickle(pk_trp)


    def __len__(self, context=None):
        assert self.__open, "The Store must be open."
        if context == self:
            context = None

        if context is not None:
            dataset = self.triples((None, None, None), context)
            return len(set(dataset))
        else:
            with self.environment.begin() as txn:
                return txn.stat(self.trp_db)['entries']


    def bind(self, prefix, namespace):
        '''
        Bind a prefix to a namespace.
        '''
        prefix = s2b(prefix)
        namespace = s2b(namespace)
        with self.txn.cursor(self.idx_db(b'ns:pfx')) as cur:
            cur.put(namespace, prefix)
        with self.txn.cursor(self.idx_db(b'pfx:ns')) as cur:
            cur.put(prefix, namespace)


    def namespace(self, prefix):
        '''
        Get the namespace for a prefix.
        '''
        ns = self.idx_cur['pfx:ns'].get(s2b(prefix))

        return URIRef(b2s(ns)) if ns is not None else None


    def prefix(self, namespace):
        '''
        Get the prefix associated with a namespace.

        @NOTE A namespace can be only bound to one prefix in this
        implementation.
        '''
        prefix = self.data_cur['ns:pfx'].get(s2b(namespace))

        return b2s(prefix) if prefix is not None else None


    def namespaces(self):
        '''
        Get a dict of all prefix: namespace bindings.
        '''
        with self.tx.cursor(self.pfx_db) as cur:
            bindings = iter(cur)

        return ((b2s(pfx), b2s(ns)) for pfx, ns in bindings)


    def contexts(self, triple=None):
        '''
        Get a list of all contexts.
        '''
        if triple:
            with self.tx.cursor(self.ctx_db) as cur:
                cur.set_key(self._to_key(triple))
                contexts = cur.iternext_dup()
        else:
            with self.tx.cursor(self.idx_db[b'c:tk']) as cur:
                contexts = cur.iternext_nodup()

        return (b2s(ctx) for ctx in contexts)


    def add_graph(self, graph):
        '''
        Add a graph to the database.
        '''
        self.data_cur['tk:c'].put(self._pickle(None), self._pickle(graph))
        self.idx_cur['c:tk'].put(self._pickle(graph), self._pickle(None))


    def remove_graph(self, graph):
        '''
        Remove all triples from graph and the graph itself.
        '''
        self.remove((None, None, None), graph)

        if self.data_cur['tk:c'].set_key_dup(
                self._pickle(None), self._pickle(graph)):
            self.data_cur['tk:c'].delete()

        if self.idx_cur['c:tk'].set_key_dup(
                self._pickle(graph), self._pickle(None)):
            self.data_cur['tk:c'].delete()


    def commit(self):
        '''
        Commit main transaction.
        '''
        self.txn.commit()


    def rollback(self):
        '''
        Roll back main transaction.
        '''
        self.txn.abort()


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
        pk_trp = self.data_cur['tk:t'].get(key)

        return self._unpickle(pk_trp) if pk_trp else None


    def _lookup(self, triple_pattern):
        '''
        Look up triples based on a triple pattern.

        This is only used if one or two terms are nubound. If all terms are
        either bound  or unbound, other methods should be used.

        @return iterator of matching triple keys.
        '''
        if not any(triple_pattern) or all(triple_pattern):
            raise ValueError(
                    'This method is not usable with a triple with only '
                    'unbound or only bound terms.')

        s, p, o = triple_pattern

        with self.env.begin(buffers=True) as txn:
            if s is None:
                if p is None:
                    cursor = self.txn.cursor(self.idx_db['o:tk'])
                    term = self._pickle(o)
                else:
                    cursor = self.txn.cursor(self.idx_db['po:tk'])
                    term = self._pickle((p, o))
            if p is None:
                if o is None:
                    cursor = self.txn.cursor(self.idx_db['s:tk'])
                    term = self._pickle(s)
                else:
                    cursor = self.txn.cursor(self.idx_db['so:tk'])
                    term = self._pickle((s, o))
            if o is None:
                if s is None:
                    cursor = self.txn.cursor(self.idx_db['p:tk'])
                    term = self._pickle(s)
                else:
                    cursor = self.txn.cursor(self.idx_db['sp:tk'])
                    term = self._pickle((s, p))

            key = hashlib.new(self.KEY_HASH_ALGO, term).digest()
            with cursor as cur:
                if cur.set_key(key):
                    for match in cur.iternext_dup():
                        yield match
                else:
                    return iter(())

