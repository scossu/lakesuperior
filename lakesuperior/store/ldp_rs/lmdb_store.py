import hashlib
import logging
import os

from contextlib import ContextDecorator, ExitStack
from os import makedirs
from os.path import exists, abspath
from shutil import rmtree
from urllib.request import pathname2url

import lmdb

from rdflib import Graph, Namespace, URIRef, Variable
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI
from rdflib.store import Store, VALID_STORE, NO_STORE

from lakesuperior import env

logger = logging.getLogger(__name__)


def s2b(u, enc='UTF-8'):
    """
    Convert a string into a bytes object.
    """
    return u.encode(enc)


def b2s(u, enc='UTF-8'):
    """
    Convert a bytes or memoryview object into a string.
    """
    return bytes(u).decode(enc)


class TxnManager(ContextDecorator):
    """
    Handle ACID transactions with an LmdbStore.

    Wrap this within a ``with`` statement:

    >>> with TxnManager(store, True):
    ...     # Do something with the database
    >>>

    The transaction will be opened and handled automatically.
    """
    def __init__(self, store, write=False):
        """
        Begin and close a transaction in a store.

        :param LmdbStore store: The store to open a transaction on.
        :param bool write: Whether the transaction is read-write. Default is
            ``False`` (read-only transaction).
        """
        self.store = store
        self.write = write

    def __enter__(self):
        # Only open a R/W transaction if one is not already open.
        if not self.write or not self.store.is_txn_rw:
            self.store.begin(write=self.write)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.store.rollback()
        else:
            self.store.commit()



class LexicalSequence:
    """
    Fixed-length lexicographically ordered byte sequence.

    Useful to generate optimized sequences of keys in LMDB.
    """
    def __init__(self, start=1, max_len=5):
        """
        Create a new lexical sequence.

        :param bytes start: Starting byte value. Bytes below this value are
            never found in this sequence. This is useful to allot special bytes
            to be used e.g. as separators.
        :param int max_len: Maximum number of bytes that a byte string can
            contain. This should be chosen carefully since the number of all
            possible key combinations is determined by this value and the
            ``start`` value. The default args provide 255**5 (~1 Tn) unique
            combinations.
        """
        self.start = start
        self.length = max_len


    def first(self):
        """First possible combination."""
        return bytearray([self.start] * self.length)


    def next(self, n):
        """
        Calculate the next closest byte sequence in lexicographical order.

        This is used to fill the next available slot after the last one in
        LMDB. Keys are byte strings, which is a convenient way to keep key
        lengths as small as possible when they are referenced in several
        indices.

        This function assumes that all the keys are padded with the `start`
        value up to the `max_len` length.

        :param bytes n: Current byte sequence to add to.
        """
        if not n:
            n = self.first()
        elif isinstance(n, bytes) or isinstance(n, memoryview):
            n = bytearray(n)
        elif not isinstance(n, bytearray):
            raise ValueError('Input sequence must be bytes or a bytearray.')

        if not len(n) == self.length:
            raise ValueError('Incorrect sequence length.')

        for i, b in list(enumerate(n))[::-1]:
            try:
                n[i] += 1
            # If the value exceeds 255, i.e. the current value is the last one
            except ValueError:
                if i == 0:
                    raise RuntimeError('BAD DAY: Sequence exhausted. No more '
                            'combinations are possible.')
                # Move one position up and try to increment that.
                else:
                    n[i] = self.start
                    continue
            else:
                return bytes(n)



class LmdbStore(Store):
    """
    LMDB-backed store.

    This is an implementation of the RDFLib Store interface:
    https://github.com/RDFLib/rdflib/blob/master/rdflib/store.py

    Handles the interaction with a LMDB store and builds an abstraction layer
    for triples.

    This store class uses two LMDB environments (i.e. two files): one for the
    main (preservation-worthy) data and the other for the index data which
    can be rebuilt from the main database.

    There are 4 main data sets (preservation worthy data):

    - ``t:st`` (term key: serialized term; 1:1)
    - ``spo:c`` (joined S, P, O keys: context key; dupsort, dupfixed)
    - ``c:`` (context keys only, values are the empty bytestring; 1:1)
    - ``pfx:ns`` (prefix: pickled namespace; 1:1)

    And 6 indices to optimize lookup for all possible bound/unbound term
    combination in a triple:

    - ``th:t`` (term hash: term key; 1:1)
    - ``s:po`` (S key: joined P, O keys; dupsort, dupfixed)
    - ``p:so`` (P key: joined S, O keys; dupsort, dupfixed)
    - ``o:sp`` (O key: joined S, P keys; dupsort, dupfixed)
    - ``c:spo`` (context → triple association; dupsort, dupfixed)
    - ``ns:pfx`` (pickled namespace: prefix; 1:1)

    The default graph is defined in
    :data:`rdflib.graph.RDFLIB_DEFAULT_GRAPH_URI`. Adding
    triples without context will add to this graph. Looking up triples without
    context (also in a SPARQL query) will look in the  union graph instead of
    in the default graph. Also, removing triples without specifying a context
    will remove triples from all contexts.
    """

    context_aware = True
    # This is a hassle to maintain for no apparent gain. If some use is devised
    # in the future, it may be revised.
    formula_aware = False
    graph_aware = True
    transaction_aware = True

    MAP_SIZE = 1024 ** 4 # 1Tb
    """
    LMDB map size. See http://lmdb.readthedocs.io/en/release/#environment-class
    """

    TERM_HASH_ALGO = 'sha1'
    """
    Term hashing algorithm. SHA1 is the default.
    """

    KEY_LENGTH = 5
    """
    Fixed length for term keys.

    4 or 5 is a safe range. 4 allows for ~4 billion (256 ** 4) unique terms
    in the store. 5 allows ~1 trillion terms. While these numbers may seem
    huge (the total number of Internet pages indexed by Google as of 2018 is 45
    billions), it must be reminded that the keys cannot be reused, so a
    repository that deletes a lot of triples may burn through a lot of terms.

    If a repository runs ot of keys it can no longer store new terms and must
    be migrated to a new database, which will regenerate and compact the keys.

    For smaller repositories it should be safe to set this value to 4, which
    could improve performance since keys make up the vast majority of record
    exchange between the store and the application. However it is sensible not
    to expose this value as a configuration option.
    """

    KEY_START = 1
    """
    Lexical sequence start. ``\\x01`` is fine since no special characters are
    used, but it's good to leave a spare for potential future use.
    """

    data_keys = (
        # Term key to serialized term content: 1:1
        't:st',
        # Joined triple keys to context key: 1:m, fixed-length values
        'spo:c',
        # This has empty values and is used to keep track of empty contexts.
        'c:',
        # Prefix to namespace: 1:1
        'pfx:ns',
    )
    idx_keys = (
        # Namespace to prefix: 1:1
        'ns:pfx',
        # Term hash to triple key: 1:1
        'th:t',
        # Lookups: 1:m, fixed-length values
        's:po', 'p:so', 'o:sp', 'c:spo',
    )

    _lookup_rank = ('s', 'o', 'p')
    """
    Order in which keys are looked up if two terms are bound.
    The indices with the smallest average number of values per key should be
    looked up first.

    If we want to get fancy, this can be rebalanced from time to time by
    looking up the number of keys in (s:po, p:so, o:sp).
    """

    _lookup_ordering = {
        's:po': (0, 1, 2),
        'p:so': (1, 0, 2),
        'o:sp': (2, 0, 1),
    }
    """
    Order of terms in the lookup indices. Used to rebuild a triple from lookup.
    """

    data_env = None
    idx_env = None
    db = None
    dbs = {}
    data_txn = None
    idx_txn = None
    is_txn_rw = None


    def __init__(self, path, identifier=None):
        self.path = path
        self.__open = False

        self.identifier = identifier or URIRef(pathname2url(abspath(path)))
        super().__init__(path)

        self._pickle = self.node_pickler.dumps
        self._unpickle = self.node_pickler.loads

        self._key_seq = LexicalSequence(self.KEY_START, self.KEY_LENGTH)


    def __del__(self):
        """Properly close store for garbage collection."""
        self.close(True)


    def __len__(self, context=None):
        """
        Return length of the dataset.

        :param context: Context to restrict count to.
        :type context: rdflib.URIRef or rdflib.Graph
        """
        context = self._normalize_context(context)

        if context is not None:
            #dataset = self.triples((None, None, None), context)
            with self.cur('c:spo') as cur:
                if cur.set_key(self._to_key(context)):
                    return sum(1 for _ in cur.iternext_dup())
                else:
                    return 0
        else:
            return self.data_txn.stat(self.dbs['spo:c'])['entries']


    @property
    def is_open(self):
        return self.__open


    def open(self, configuration=None, create=True):
        """
        Open the database.

        The database is best left open for the lifespan of the server. Read
        transactions can be opened as needed. Write transaction should be
        opened and closed within a single HTTP request to ensure atomicity of
        the request.

        This method is called outside of the main transaction. All cursors
        are created separately within the transaction.
        """
        self._init_db_environments(create)
        if self.data_env == NO_STORE:
            return NO_STORE
        self.__open = True

        return VALID_STORE


    def begin(self, write=False):
        """
        Begin the main write transaction and create cursors.
        """
        if not self.is_open:
            raise RuntimeError('Store must be opened first.')
        logger.debug('Beginning a {} transaction.'.format(
            'read/write' if write else 'read-only'))

        self.data_txn = self.data_env.begin(buffers=True, write=write)
        self.idx_txn = self.idx_env.begin(buffers=True, write=write)

        self.is_txn_rw = write


    def stats(self):
        """Gather statistics about the database."""
        stats = {
            'data_db_stats': {
                db_label: self.data_txn.stat(self.dbs[db_label])
                for db_label in self.data_keys},

            'idx_db_stats': {
                db_label: self.idx_txn.stat(self.dbs[db_label])
                for db_label in self.idx_keys},

            'data_db_size': os.stat(self.data_env.path()).st_size,
            'idx_db_size': os.stat(self.idx_env.path()).st_size,
            'num_triples': len(self),
        }

        return stats


    @property
    def is_txn_open(self):
        """Whether the main transaction is open."""
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
        """Return a new cursor by its index."""
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
        """
        Build the main data cursors for a transaction.

        :param lmdb.Transaction txn: This can be a read or write transaction.

        :rtype: dict(string, lmdb.Cursor)
        :return: Keys are index labels, values are index cursors.
        """
        return {
            'tk:t': txn.cursor(self.dbs['tk:t']),
            'tk:c': txn.cursor(self.dbs['tk:c']),
            'pfx:ns': txn.cursor(self.dbs['pfx:ns']),
        }


    def get_idx_cursors(self, txn):
        """
        Build the index cursors for a transaction.

        :param lmdb.Transaction txn: This can be a read or write transaction.

        :rtype: dict(string, lmdb.Cursor)
        :return: dict of index labels, index cursors.
        """
        return {
            key: txn.cursor(self.dbs[key])
            for key in self.idx_keys}


    def close(self, commit_pending_transaction=False):
        """
        Close the database connection.

        Do this at server shutdown.
        """
        self.__open = False
        if self.is_txn_open:
            if commit_pending_transaction:
                self.commit()
            else:
                self.rollback()

        self.data_env.close()
        self.idx_env.close()


    def destroy(self, path):
        """
        Destroy the store.

        https://www.youtube.com/watch?v=lIVq7FJnPwg

        :param str path: Path of the folder containing the database(s).
        """
        if exists(path):
            rmtree(path)


    def add(self, triple, context=None, quoted=False):
        """
        Add a triple and start indexing.

        :param tuple(rdflib.Identifier) triple: Tuple of three identifiers.
        :param context: Context identifier. ``None`` inserts in the default
            graph.
        :type context: rdflib.Identifier or None
        :param bool quoted: Not used.
        """
        context = self._normalize_context(context)
        if context is None:
            context = RDFLIB_DEFAULT_GRAPH_URI

        Store.add(self, triple, context)

        #logger.info('Adding triple: {}'.format(triple))
        pk_trp = self._pickle(triple)

        pk_s, pk_p, pk_o = [self._pickle(t) for t in triple]
        #logger.debug('Adding quad: {} {}'.format(triple, context))
        pk_c = self._pickle(context)

        # Add new individual terms or gather keys for existing ones.
        keys = [None] * 4
        with self.cur('th:t') as icur:
            for i, pk_t in enumerate((pk_s, pk_p, pk_o, pk_c)):
                thash = self._hash(pk_t)
                if icur.set_key(thash):
                    keys[i] = bytes(icur.value())
                else:
                    # Put new term.
                    with self.cur('t:st') as dcur:
                        keys[i] = self._append(dcur, (pk_t,))[0]
                    # Index.
                    icur.put(thash, keys[i])

        # Add context in context DB.
        ck = keys[3]
        with self.cur('c:') as cur:
            if not cur.set_key(ck):
                cur.put(ck, b'')

        # Add triple:context association.
        spok = b''.join(keys[:3])
        with self.cur('spo:c') as dcur:
            if not dcur.set_key_dup(spok, ck):
                dcur.put(spok, ck)
        # Index spo:c association.
        with self.cur('c:spo') as icur:
            icur.put(ck, spok)

        self._index_triple('add', spok)


    def remove(self, triple_pattern, context=None):
        """
        Remove triples by a pattern.

        :param tuple:rdflib.term.Identifier|None triple_pattern: 3-tuple of
        either RDF terms or None, indicating the triple(s) to be removed.
        None is used as a wildcard.
        :param context: Context to remove the triples from. If None (the
        default) the matching triples are removed from all contexts.
        :type context: rdflib.term.Identifier or None
        """
        #logger.debug('Removing triples by pattern: {} on context: {}'.format(
        #    triple_pattern, context))
        context = self._normalize_context(context)
        if context is not None:
            ck = self._to_key(context)
            # If context is specified but not found, return to avoid deleting
            # the wrong triples.
            if not ck:
                return
        else:
            ck = None

        with self.cur('spo:c') as dcur:
            with self.cur('c:spo') as icur:
                match_set = {bytes(k) for k in self._triple_keys(
                        triple_pattern, context)}
                # Delete context association.
                if ck:
                    for spok in match_set:
                        if dcur.set_key_dup(spok, ck):
                            dcur.delete()
                            if icur.set_key_dup(ck, spok):
                                icur.delete()
                            self._index_triple('remove', spok)
                # If no context is specified, remove all associations.
                else:
                    for spok in match_set:
                        if dcur.set_key(spok):
                            for cck in (bytes(k) for k in dcur.iternext_dup()):
                                # Delete index first while we have the
                                # context reference.
                                if icur.set_key_dup(cck, spok):
                                    icur.delete()
                            # Then delete the main entry.
                            dcur.set_key(spok)
                            dcur.delete(dupdata=True)
                            self._index_triple('remove', spok)


    def triples(self, triple_pattern, context=None):
        """
        Generator over matching triples.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph, if available.
        :type context: rdflib.Graph or None

        :rtype: Iterator
        :return: Generator over triples and contexts in which each result has
            the following format::

                (s, p, o), generator(contexts)

        Where the contexts generator lists all context that the triple appears
        in.
        """
        #logger.debug('Getting triples for pattern: {} and context: {}'.format(
        #    triple_pattern, context))
        # This sounds strange, RDFLib should be passing None at this point,
        # but anyway...
        context = self._normalize_context(context)

        with self.cur('spo:c') as cur:
            for spok in self._triple_keys(triple_pattern, context):
                if context is not None:
                    contexts = (Graph(identifier=context),)
                else:
                    if cur.set_key(spok):
                        contexts = tuple(
                            Graph(identifier=self._from_key(ck)[0], store=self)
                            for ck in cur.iternext_dup())

                #print('Found triples: {} In contexts: {}'.format(
                #    self._from_key(spok), contexts))
                yield self._from_key(spok), contexts


    def all_terms(self, term_type):
        """
        Return all terms of a type (``s``, ``p``, or ``o``) in the store.

        :param str term_type: one of ``s``, ``p`` or ``o``.

        :rtype: Iterator(rdflib.term.Identifier)
        :return: Iterator of all terms.
        :raise ValueError: if the term type is not one of the expected values.
        """
        if term_type == 's':
            idx_label = 's:po'
        elif term_type == 'p':
            idx_label = 'p:so'
        elif term_type == 'o':
            idx_label = 'o:sp'
        else:
            raise ValueError('Term type must be \'s\', \'p\' or \'o\'.')

        with self.cur(idx_label) as cur:
            for key in cur.iternext_nodup():
                yield self._from_key(key)[0]


    def bind(self, prefix, namespace):
        """
        Bind a prefix to a namespace.

        :param str prefix: Namespace prefix.
        :param rdflib.URIRef namespace: Fully qualified URI of namespace.
        """
        prefix = s2b(prefix)
        namespace = s2b(namespace)
        if self.is_txn_rw:
            with self.data_txn.cursor(self.dbs['pfx:ns']) as cur:
                cur.put(prefix, namespace)
            with self.idx_txn.cursor(self.dbs['ns:pfx']) as cur:
                cur.put(namespace, prefix)
        else:
            with self.data_env.begin(write=True) as wtxn:
                with wtxn.cursor(self.dbs['pfx:ns']) as cur:
                    cur.put(prefix, namespace)
            with self.idx_env.begin(write=True) as wtxn:
                with wtxn.cursor(self.dbs['ns:pfx']) as cur:
                    cur.put(namespace, prefix)


    def namespace(self, prefix):
        """
        Get the namespace for a prefix.
        :param str prefix: Namespace prefix.
        """
        with self.cur('pfx:ns') as cur:
            ns = cur.get(s2b(prefix))
            return Namespace(b2s(ns)) if ns is not None else None


    def prefix(self, namespace):
        """
        Get the prefix associated with a namespace.

        **Note:** A namespace can be only bound to one prefix in this
        implementation.

        :param rdflib.Namespace namespace: Fully qualified namespace.

        :rtype: str or None
        """
        with self.cur('ns:pfx') as cur:
            prefix = cur.get(s2b(namespace))
            return b2s(prefix) if prefix is not None else None


    def namespaces(self):
        """Get an iterator of all prefix: namespace bindings.

        :rtype: Iterator(tuple(str, rdflib.Namespace))
        """
        with self.cur('pfx:ns') as cur:
            for pfx, ns in iter(cur):
                yield (b2s(pfx), Namespace(b2s(ns)))


    def contexts(self, triple=None):
        """
        Get a list of all contexts.

        :rtype: Iterator(rdflib.Graph)
        """
        if triple and any(triple):
            with self.cur('spo:c') as cur:
                if cur.set_key(self._to_key(triple)):
                    for ctx_uri in cur.iternext_dup():
                        yield Graph(
                            identifier=self._from_key(ctx_uri)[0], store=self)
        else:
            with self.cur('c:') as cur:
                for ctx_uri in cur.iternext(values=False):
                    yield Graph(
                            identifier=self._from_key(ctx_uri)[0], store=self)


    def add_graph(self, graph):
        """
        Add a graph to the database.

        This creates an empty graph by associating the graph URI with the
        pickled `None` value. This prevents from removing the graph when all
        triples are removed.

        This may be called by read-only operations:
        https://github.com/RDFLib/rdflib/blob/master/rdflib/graph.py#L1623
        Therefore it needs to open a write transaction. This is not ideal
        but the only way to handle datasets in RDFLib.

        :param rdflib.URIRef graph: URI of the named graph to add.
        """
        if isinstance(graph, Graph):
            graph = graph.identifier
        pk_c = self._pickle(graph)
        c_hash = self._hash(pk_c)
        with self.cur('th:t') as cur:
            c_exists = cur.set_key(c_hash)
        if not c_exists:
            # Insert context term if not existing.
            if self.is_txn_rw:
                # Use existing R/W transaction.
                with self.cur('t:st') as cur:
                    ck = self._append(cur, (pk_c,))[0]
                with self.cur('th:t') as cur:
                    cur.put(c_hash, ck)
                with self.cur('c:') as cur:
                    cur.put(ck, b'')
            else:
                # Open new R/W transactions.
                with self.data_env.begin(write=True) as wtxn:
                    with wtxn.cursor(self.dbs['t:st']) as cur:
                        ck = self._append(cur, (pk_c,))[0]
                    with wtxn.cursor(self.dbs['c:']) as cur:
                        cur.put(ck, b'')
                with self.idx_env.begin(write=True) as wtxn:
                    with wtxn.cursor(self.dbs['th:t']) as cur:
                        cur.put(c_hash, ck)


    def remove_graph(self, graph):
        """
        Remove all triples from graph and the graph itself.

        :param rdflib.URIRef graph: URI of the named graph to remove.
        """
        if isinstance(graph, Graph):
            graph = graph.identifier
        self.remove((None, None, None), graph)

        with self.cur('c:') as cur:
            if cur.set_key(self._to_key(graph)):
                cur.delete()


    def commit(self):
        """Commit main transaction."""
        logger.debug('Committing transaction.')
        try:
            self.data_txn.commit()
        except (AttributeError, lmdb.Error):
            pass
        try:
            self.idx_txn.commit()
        except (AttributeError, lmdb.Error):
            pass
        self.is_txn_rw = None


    def rollback(self):
        """Roll back main transaction."""
        logger.debug('Rolling back transaction.')
        try:
            self.data_txn.abort()
        except (AttributeError, lmdb.Error):
            pass
        try:
            self.idx_txn.abort()
        except (AttributeError, lmdb.Error):
            pass
        self.is_txn_rw = None


    ## PRIVATE METHODS ##

    def _triple_keys(self, triple_pattern, context=None):
        """
        Generator over matching triple keys.

        This method is used by `triples` which returns native Python tuples,
        as well as by other methods that need to iterate and filter triple
        keys without incurring in the overhead of converting them to triples.

        :param tuple triple_pattern: 3 RDFLib terms
        :param context: Context graph or URI, or None.
        :type context: rdflib.term.Identifier or None
        """
        if context == self:
            context = None

        if context is not None:
            pk_c = self._pickle(context)
            ck = self._to_key(context)

            # Shortcuts
            if not ck:
                # Context not found.
                return iter(())

            with self.cur('c:spo') as cur:
                # s p o c
                if all(triple_pattern):
                    spok = self._to_key(triple_pattern)
                    if not spok:
                        # A term in the triple is not found.
                        return iter(())
                    if cur.set_key_dup(ck, spok):
                        yield spok
                        return
                    else:
                        # Triple not found.
                        return iter(())

                # ? ? ? c
                elif not any(triple_pattern):
                    # Get all triples from the context
                    if cur.set_key(ck):
                        for spok in cur.iternext_dup():
                            yield spok
                    else:
                        return iter(())

                # Regular lookup.
                else:
                    yield from (
                            spok for spok in self._lookup(triple_pattern)
                            if cur.set_key_dup(ck, spok))
        else:
            yield from self._lookup(triple_pattern)


    def _init_db_environments(self, create=True):
        """
        Initialize the DB environment.

        The main database is kept in one file, the indices in a separate one
        (these may be even further split up depending on performance
        considerations).

        :param bool create: If True, the environment and its databases are
        created.
        """
        path = self.path
        if not exists(path):
            if create is True:
                makedirs(path)
            else:
                return NO_STORE

        if getattr(env, 'wsgi_options', False):
            self._workers = env.wsgi_options['workers']
        else:
            self._workers = 1
        logger.info('Max LMDB readers: {}'.format(self._workers))

        self.data_env = lmdb.open(
                path + '/main', subdir=False, create=create,
                map_size=self.MAP_SIZE, max_dbs=4,
                max_spare_txns=self._workers, readahead=False)
        self.idx_env = lmdb.open(
                path + '/index', subdir=False, create=create,
                map_size=self.MAP_SIZE, max_dbs=6,
                max_spare_txns=self._workers, readahead=False)

        # Clear stale readers.
        data_stale_readers = self.data_env.reader_check()
        idx_stale_readers = self.idx_env.reader_check()
        logger.debug(
                'Cleared data stale readers: {}'.format(data_stale_readers))
        logger.debug(
                'Cleared index stale readers: {}'.format(idx_stale_readers))

        # Open and optionally create main databases.
        self.dbs = {
            # Main databases.
            't:st': self.data_env.open_db(b't:st', create=create),
            'spo:c': self.data_env.open_db(
                    b'spo:c', create=create, dupsort=True, dupfixed=True),
            'c:': self.data_env.open_db(b'c:', create=create),
            'pfx:ns': self.data_env.open_db(b'pfx:ns', create=create),
            # One-off indices.
            'ns:pfx': self.idx_env.open_db(b'ns:pfx', create=create),
            'th:t': self.idx_env.open_db(b'th:t', create=create),
        }
        # Other index databases.
        for db_key in self.idx_keys:
            if db_key not in ('ns:pfx', 'th:t'):
                self.dbs[db_key] = self.idx_env.open_db(s2b(db_key),
                        dupsort=True, dupfixed=True, create=create)


    def _from_key(self, key):
        """
        Convert a key into one or more terms.

        :param key: The key to be converted. It can be a
        :type key: bytes or memoryview
        compound one in which case the function will return multiple terms.

        :rtype: tuple(rdflib.term.Identifier)
        :return: The term(s) associated with the key(s). The result is always
        a tuple even for single results.
        """
        with self.cur('t:st') as cur:
            return tuple(
                   self._unpickle(cur.get(k))
                   for k in self._split_key(key))


    def _to_key(self, obj):
        """
        Convert a triple, quad or term into a key.

        The key is the checksum of the pickled object, therefore unique for
        that object. The hashing algorithm is specified in `TERM_HASH_ALGO`.

        :param Object obj: Anything that can be reduced to terms stored in the
        database. Pairs of terms, as well as triples and quads, are expressed
        as tuples.

        If more than one term is provided, the keys are concatenated.

        :rtype: memoryview
        :return: Keys stored for the term(s)
        """
        if not isinstance(obj, list) and not isinstance(obj, tuple):
            obj = (obj,)
        key = []
        with self.cur('th:t') as cur:
            for term in obj:
                tk = cur.get(self._hash(self._pickle(term)))
                if not tk:
                    # If any of the terms is not found, return None immediately
                    return None
                key.append(tk)

        return b''.join(key)


    def _hash(self, s):
        """Get the hash value of a serialized object."""
        return hashlib.new(self.TERM_HASH_ALGO, s).digest()


    def _split_key(self, keys):
        """
        Split a compound key into individual keys.

        This method relies on the fixed length of all term keys.

        :param keys: Concatenated keys.
        :type keys: bytes or memoryview

        :rtype: tuple(memoryview)
        """
        return tuple(
                keys[i:i+self.KEY_LENGTH]
                for i in range(0, len(keys), self.KEY_LENGTH))


    def _normalize_context(self, context):
        """
        Normalize a context parameter to conform to the model expectations.

        :param context: Context URI or graph.
        :type context: URIRef or Graph or None
        """
        if isinstance(context, Graph):
            if context == self or isinstance(context.identifier, Variable):
                context = None
            else:
                context = context.identifier
                #logger.debug('Converted graph into URI: {}'.format(context))

        return context


    def _lookup(self, triple_pattern):
        """
        Look up triples in the indices based on a triple pattern.

        :rtype: Iterator
        :return: Matching triple keys.
        """
        s, p, o = triple_pattern

        if s is not None:
            if p is not None:
                # s p o
                if o is not None:
                    with self.cur('spo:c') as cur:
                        tkey = self._to_key(triple_pattern)
                        if cur.set_key(tkey):
                            yield tkey
                            return
                        else:
                            return iter(())
                # s p ?
                else:
                    yield from self._lookup_2bound({'s': s, 'p': p})
            else:
                # s ? o
                if o is not None:
                    yield from self._lookup_2bound({'s': s, 'o': o})
                # s ? ?
                else:
                    yield from self._lookup_1bound('s:po', s)
        else:
            if p is not None:
                # ? p o
                if o is not None:
                    yield from self._lookup_2bound({'p': p, 'o': o})
                # ? p ?
                else:
                    yield from self._lookup_1bound('p:so', p)
            else:
                # ? ? o
                if o is not None:
                    yield from self._lookup_1bound('o:sp', o)
                # ? ? ?
                else:
                    # Get all triples in the database.
                    with self.cur('spo:c') as cur:
                        yield from cur.iternext_nodup()


    def _lookup_1bound(self, idx_name, term):
        """
        Lookup triples for a pattern with one bound term.

        :param str idx_name: The index to look up as one of the keys of
            ``_lookup_ordering``.
        :param rdflib.URIRef term: Bound term to search for.

        :rtype: Iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        k = self._to_key(term)
        if not k:
            return iter(())
        term_order = self._lookup_ordering[idx_name]
        with self.cur(idx_name) as cur:
            if cur.set_key(k):
                for match in cur.iternext_dup():
                    subkeys = self._split_key(match)

                    # Compose result.
                    out = [None] * 3
                    out[term_order[0]] = k
                    out[term_order[1]] = subkeys[0]
                    out[term_order[2]] = subkeys[1]

                    yield b''.join(out)


    def _lookup_2bound(self, bound_terms):
        """
        Look up triples for a pattern with two bound terms.

        :param  bound: terms (dict) Triple labels and terms to search for,
        in the format of, e.g. {'s': URIRef('urn:s:1'), 'o':
        URIRef('urn:o:1')}

        :rtype: iterator(bytes)
        :return: SPO keys matching the pattern.
        """
        if len(bound_terms) != 2:
            raise ValueError(
                    'Exactly 2 terms need to be bound. Got {}'.format(
                        len(bound_terms)))

        # Establish lookup ranking.
        luc = None
        for k_label in self._lookup_rank:
            if k_label in bound_terms.keys():
                # First match is lookup term.
                if not luc:
                    v_label = 'spo'.replace(k_label, '')
                    # Lookup database key (cursor) name
                    luc = k_label + ':' + v_label
                    term_order = self._lookup_ordering[luc]
                    # Term to look up
                    luk = self._to_key(bound_terms[k_label])
                    if not luk:
                        return iter(())
                    # Position of key in final triple.
                # Second match is the filter.
                else:
                    # Filter key (position of sub-key in lookup results)
                    fpos = v_label.index(k_label)
                    # Fliter term
                    ft = self._to_key(bound_terms[k_label])
                    if not ft:
                        return iter(())
                    break

        # Look up in index.
        with self.cur(luc) as cur:
            if cur.set_key(luk):
                # Iterate over matches and filter by second term.
                for match in cur.iternext_dup():
                    subkeys = self._split_key(match)
                    flt_subkey = subkeys[fpos]
                    if flt_subkey == ft:
                        # Remainder (not filter) key used to complete the
                        # triple.
                        r_subkey = subkeys[1-fpos]

                        # Compose result.
                        out = [None, None, None]
                        out[term_order[0]] = luk
                        out[term_order[fpos+1]] = flt_subkey
                        out[term_order[2-fpos]] = r_subkey

                        yield b''.join(out)


    def _append(self, cur, values, **kwargs):
        """
        Append one or more values to the end of a database.

        :param lmdb.Cursor cur: The write cursor to act on.
        :param list(bytes) values: Value(s) to append.

        :rtype: list(memoryview)
        :return: Last key(s) inserted.
        """
        if not isinstance(values, list) and not isinstance(values, tuple):
            raise ValueError('Input must be a list or tuple.')
        data = []
        lastkey = cur.key() if cur.last() else None
        for v in values:
            lastkey = self._key_seq.next(lastkey)
            data.append((lastkey, v))

        cur.putmulti(data, **kwargs)

        return [d[0] for d in data]


    def _index_triple(self, action, spok):
        """
        Update index for a triple and context (add or remove).

        :param str action: 'add' or 'remove'.
        :param bytes spok: Triple key.
        """
        # Split and rearrange-join keys for association and indices.
        triple = self._split_key(spok)
        sk, pk, ok = triple
        spk = b''.join(triple[:2])
        spk = b''.join(triple[:2])
        sok = b''.join((triple[0], triple[2]))
        pok = b''.join(triple[1:3])

        # Associate cursor labels with k/v pairs.
        curs = {
            's:po': (sk, pok),
            'p:so': (pk, sok),
            'o:sp': (ok, spk),
        }

        # Add or remove triple lookups.
        for clabel, terms in curs.items():
            with self.cur(clabel) as icur:
                if action == 'remove':
                    if icur.set_key_dup(*terms):
                        icur.delete()
                elif action == 'add':
                    icur.put(*terms)
                else:
                    raise ValueError(
                        'Index action \'{}\' is not supported.'.format(action))


    ## Convenience methods—not necessary for functioning but useful for
    ## debugging.

    def _keys_in_ctx(self, pk_ctx):
        """
        Convenience method to list all keys in a context.

        :param bytes pk_ctx: Pickled context URI.

        :rtype: Iterator(tuple)
        :return: Generator of triples.
        """
        with self.cur('c:spo') as cur:
            if cur.set_key(pk_ctx):
                tkeys = cur.iternext_dup()
                return {self._key_to_triple(tk) for tk in tkeys}
            else:
                return set()


    def _ctx_for_key(self, tkey):
        """
        Convenience method to list all contexts that a key is in.

        :param bytes tkey: Triple key.

        :rtype: Iterator(rdflib.URIRef)
        :return: Generator of context URIs.
        """
        with self.cur('spo:c') as cur:
            if cur.set_key(tkey):
                ctx = cur.iternext_dup()
                return {self._unpickle(c) for c in ctx}
            else:
                return set()
