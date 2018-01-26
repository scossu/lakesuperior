import hashlib
import logging

from contextlib import ExitStack
from os import makedirs
from os.path import exists, abspath
from urllib.request import pathname2url

import lmdb

from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib import Namespace, URIRef


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


def read_tx(dbs=(), buffers=True):
    '''
    Decorator to wrap a method into a read transaction.

    This method creates the necessary cursors indicated in the `db` parameter.

    @param dbs (tuple|list:string) Database label(s) to open cursors. No
    cursors are automatically opened by default.
    '''
    def read_tx_deco(fn):
        def wrapper(self, *args, **kwargs):
            with ExitStack() as stack:
                self.rtxn = stack.enter_context(
                        self.db_env.begin(buffers=buffers))
                self.rcurs = {}
                for db_label in dbs:
                    self.rcurs[db_label] = stack.enter_context(
                            self.rtxn.cursor(self.dbs[db_label]))
                stack.pop_all()
                ret = fn(self, *args, **kwargs)
                stack.close()
                return ret
        return wrapper
    return read_tx_deco


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
    - pfx:ns (pickled prefix URI: namespace string; unique)

    And 7 indices to optimize lookup for all possible bound/unbound term
    combination in a triple:

    - c:tk (pickled context URI: triple key)
    - sk:tk (subject key: triple key)
    - pk:tk (pred key: triple key)
    - ok:tk (object key: triple key)
    - spk:tk (subject + predicate key: triple key)
    - sok:tk (subject + object key: triple key)
    - pok:tk (predicate + object key: triple key)
    - ns:pfx (namespace: pickled prefix URI; unique)

    The above indices (except for ns:pfx) are all multi-valued and store
    fixed-length hash values referring to triples for economy's sake.

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
    dbs = {}
    wtxn = None


    def __init__(self, path, identifier=None):
        self.__open = False
        self.identifier = identifier
        super(LmdbStore, self).__init__(path)

        self._pickle = self.node_pickler.dumps
        self._unpickle = self.node_pickler.loads


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
        self.wtxn = self.db_env.begin(write=True, buffers=True)
        # Cursors.
        self.wcurs = self.get_data_cursors(self.wtxn)
        self.wcurs.update(self.get_idx_cursors(self.wtxn))


    @property
    def is_rtxn_open(self):
        '''
        Whether the main read transaction is open.
        '''
        try:
            self.rtxn.id()
        except (lmdb.Error, AttributeError):
            logger.info('Read transaction does not exist or is closed.')
            return False
        else:
            logger.info('Read transaction is open.')
            return True


    @property
    def is_wtxn_open(self):
        '''
        Whether the main write transaction is open.
        '''
        try:
            self.wtxn.id()
        except (lmdb.Error, AttributeError):
            logger.info('Write transaction does not exist or is closed.')
            return False
        else:
            logger.info('Write transaction is open.')
            return True


    @property
    def txn(self):
        '''
        Get current active transaction for read-only use.

        @return lmdb.Transaction|None Return the main read transaction or the
        main write transaction, whichever is open, or None if neither is
        open.
        '''
        if self.is_rtxn_open:
            return self.rtxn
        elif self.is_wtxn_open:
            return self.wtxn
        else:
            return None


    @property
    def curs(self):
        '''
        Get cursor list for the current active transaction. See txn.

        @return dict:lmdb.Cursor
        '''
        if self.is_rtxn_open:
            return self.rcurs
        if self.is_wtxn_open:
            return self.wcurs
        else:
            return None


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
            'pfx:ns': txn.cursor(self.dbs['ns:pfx']),
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
            cur[key] = self.wtxn.cursor(self.dbs[key])

        return cur


    def close(self, commit_pending_transaction=False):
        '''
        Close the database connection.

        Do this at server shutdown.
        '''
        self.__open = False
        if self.is_wtxn_open:
            if commit_pending_transaction:
                self.commit()
            else:
                self.rollback()
            self.wtxn = None

        self.db_env.close()


    def add(self, triple, context=None):
        '''
        Add a triple and start indexing.

        @param triple (tuple:rdflib.Identifier) Tuple of three identifiers.
        @param context (rdflib.Identifier | None) Context identifier.
        'None' inserts in the default graph.
        '''
        assert context != self, "Can not add triple directly to store"
        Store.add(self, triple, context)

        if self.DEFAULT_UNION:
            raise NotImplementedError()
            # @TODO
        else:
            context = context or self.DEFAULT_GRAPH_URI
        pk_trp = self._pickle(triple)
        trp_key = hashlib.new(self.KEY_HASH_ALGO, pk_trp).digest()

        needs_indexing = False
        if self.wcurs['tk:t'].put(trp_key, pk_trp, overwrite=False):
            needs_indexing = True

        pk_ctx = self._pickle(context)
        if not self.wcurs['tk:c'].set_key_dup(trp_key, pk_ctx):
            self.wcurs['tk:c'].put(trp_key, pk_ctx)
            needs_indexing = True

        if needs_indexing:
            # @TODO make await
            self._update_indices(triple, trp_key, pk_ctx)


    def remove(self, triple_pattern, context=None):
        '''
        Remove a triple and start indexing.
        '''
        if self.DEFAULT_UNION:
            raise NotImplementedError()
            # @TODO
        else:
            context = context or self.DEFAULT_GRAPH_URI
        pk_ctx = self._pickle(context)
        for trp in self.triples(triple_pattern, context):
            trp_key = self._to_key(trp)

            import pdb; pdb.set_trace()
            # Delete context association.
            if self.wcurs['tk:c'].set_key_dup(trp_key, pk_ctx):
                self.wcurs['tk:c'].delete()

                # If no other contexts are associated w/ the triple, delete it.
                if not self.wcurs['tk:c'].set_key(trp_key) and (
                        self.wcurs['tk:t'].set_key(trp_key)):
                    self.wcurs['tk:t'].delete()

                # @TODO make await
                self._update_indices(trp, trp_key, pk_ctx)


    # @TODO Make async
    def _update_indices(self, triple, trp_key, pk_ctx):
        '''
        Update indices for a given triple.

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

        if self.wcurs['tk:t'].get(trp_key):
            # Add to index.
            for ikey in term_keys:
                self.wcurs[ikey].put(term_keys[ikey], trp_key)
        else:
            # Delete from index if a match is found.
            for ikey in term_keys:
                if self.wcurs[ikey].set_key_dup(term_keys[ikey], trp_key):
                    self.wcurs[ikey].delete()

        # Add or remove context association index.
        if self.wcurs['tk:c'].set_key_dup(trp_key, pk_ctx):
            self.wcurs['c:tk'].put(pk_ctx, trp_key)
        elif self.wcurs['c:tk'].set_key_dup(pk_ctx, trp_key):
            self.wcurs['c:tk'].delete()


    @read_tx((
        'sk:tk', 'pk:tk', 'ok:tk', 'spk:tk', 'sok:tk', 'pok:tk',
        'c:tk', 'tk:c', 'tk:t'))
    def triples(self, triple_pattern, context=None):
        '''
        Generator over matching triples.
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
        else:
            context = context or self.DEFAULT_GRAPH_URI

        tkey = self._to_key(triple_pattern)

        # Shortcuts
        pk_ctx = self._pickle(context)
        if not self.rcurs['c:tk'].set_key(pk_ctx):
            # Context not found.
            return iter(())

        # s p o c
        if all(triple_pattern):
            if self.rcurs['tk:c'].set_key_dup(tkey, pk_ctx):
                yield self._key_to_triple(tkey)
                return
            else:
                # Triple not found.
                return iter(())

        # ? ? ? c
        elif not any(triple_pattern):
            # Get all triples from the context
            for tk in self.rcurs['c:tk'].iternext_dup():
                yield self._key_to_triple(tk)

        # Regular lookup.
        else:
            for tk in self._lookup(triple_pattern, tkey):
                if self.rcurs['c:tk'].set_key_dup(pk_ctx, tk):
                    yield self._key_to_triple(tk)


    @read_tx()
    def __len__(self, context=None):
        '''
        Return length of the dataset.
        '''
        if context == self:
            context = None
        context = context or self.DEFAULT_GRAPH_URI

        if context is not self.DEFAULT_GRAPH_URI:
            dataset = self.triples((None, None, None), context)
            return len(set(dataset))
        else:
            return self.rtxn.stat(self.dbs['tk:t'])['entries']


    def bind(self, prefix, namespace):
        '''
        Bind a prefix to a namespace.
        '''
        prefix = s2b(prefix)
        namespace = s2b(namespace)
        with self.wtxn.cursor(self.dbs['ns:pfx']) as cur:
            cur.put(namespace, prefix)
        with self.wtxn.cursor(self.dbs['pfx:ns']) as cur:
            cur.put(prefix, namespace)


    @read_tx(('pfx:ns',))
    def namespace(self, prefix):
        '''
        Get the namespace for a prefix.
        '''
        ns = self.rcurs['pfx:ns'].get(s2b(prefix))

        return Namespace(b2s(ns)) if ns is not None else None


    @read_tx(('ns:pfx',))
    def prefix(self, namespace):
        '''
        Get the prefix associated with a namespace.

        @NOTE A namespace can be only bound to one prefix in this
        implementation.
        '''
        prefix = self.rcurs['ns:pfx'].get(s2b(namespace))

        return b2s(prefix) if prefix is not None else None


    @read_tx(('pfx:ns',))
    def namespaces(self):
        '''
        Get an iterator of all prefix: namespace bindings.
        '''
        bindings = iter(self.rcurs['pfx:ns'])

        return ((b2s(pfx), Namespace(b2s(ns))) for pfx, ns in bindings)


    @read_tx(('tk:c','c:tk'))
    def contexts(self, triple=None):
        '''
        Get a list of all contexts.

        @return generator:URIRef
        '''
        if triple:
            self.rcurs['tk:c'].set_key(self._to_key(triple))
            contexts = self.rcurs['tk:c'].iternext_dup()
        else:
            contexts = self.rcurs['c:tk'].iternext_nodup()

        return (self._unpickle(ctx) for ctx in contexts)


    def add_graph(self, graph):
        '''
        Add a graph to the database.

        @param graph (URIRef) URI of the named graph to add.
        '''
        self.wcurs['tk:c'].put(self._pickle(None), self._pickle(graph))
        self.wcurs['c:tk'].put(self._pickle(graph), self._pickle(None))


    def remove_graph(self, graph):
        '''
        Remove all triples from graph and the graph itself.

        @param graph (URIRef) URI of the named graph to remove.
        '''
        self.remove((None, None, None), graph)

        if self.wcurs['tk:c'].set_key_dup(
                self._pickle(None), self._pickle(graph)):
            self.wcurs['tk:c'].delete()

        if self.wcurs['c:tk'].set_key_dup(
                self._pickle(graph), self._pickle(None)):
            self.wcurs['tk:c'].delete()


    def commit(self):
        '''
        Commit main write transaction.
        '''
        self.wtxn.commit()


    def rollback(self):
        '''
        Roll back main write transaction.
        '''
        self.wtxn.abort()


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
        self.dbs = {
            # Main databases.
            'tk:t': self.db_env.open_db(b'tk:t', create=create),
            'tk:c': self.db_env.open_db(b'tk:c', create=create, dupsort=True),
            'pfx:ns': self.db_env.open_db(b'pfx:ns', create=create),
            # Index.
            'ns:pfx': self.db_env.open_db(b'ns:pfx', create=create),
        }
        # Other index databases.
        for db_key in self.idx_keys:
            self.dbs[db_key] = self.db_env.open_db(s2b(db_key),
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
        pk_trp = self.rcurs['tk:t'].get(key)

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
                    if self.rcurs['tk:t'].set_key(tkey):
                        yield tkey
                        return
                    else:
                        return iter(())
                # s p ?
                else:
                    cur = self.rcurs['spk:tk']
                    term = self._pickle((s, p))
            else:
                # s ? o
                if o is not None:
                    cur = self.rcurs['sok:tk']
                    term = self._pickle((s, o))
                # s ? ?
                else:
                    cur = self.rcurs['sk:tk']
                    term = self._pickle(s)
        else:
            if p is not None:
                # ? p o
                if o is not None:
                    cur = self.rcurs['pok:tk']
                    term = self._pickle((p, o))
                # ? p ?
                else:
                    cur = self.rcurs['pk:tk']
                    term = self._pickle(p)
            else:
                # ? ? o
                if o is not None:
                    cur = self.rcurs['ok:tk']
                    term = self._pickle(o)
                # ? ? ?
                else:
                    # Get all triples in the database
                    for c in self.rcurs['tk:t'].iternext(values=False):
                        yield c
                    return

        key = hashlib.new(self.KEY_HASH_ALGO, term).digest()
        if cur.set_key(key):
            for match in cur.iternext_dup():
                yield match
        else:
            return iter(())

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
