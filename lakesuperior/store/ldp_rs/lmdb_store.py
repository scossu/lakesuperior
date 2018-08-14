import hashlib
import logging
import os

from contextlib import ContextDecorator, ExitStack
from os import makedirs
from os.path import abspath
from urllib.request import pathname2url

import lmdb

from rdflib import Graph, Namespace, URIRef, Variable
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID as RDFLIB_DEFAULT_GRAPH_URI
from rdflib.store import Store, VALID_STORE, NO_STORE

from lakesuperior import env
from lakesuperior.store.ldp_rs.lmdb_triplestore import LmdbTriplestore

logger = logging.getLogger(__name__)


class LmdbStore(LmdbTriplestore, Store):
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


    def __init__(self, path, identifier=None, create=True):
        super().__init__(path, open_env=True, create=create)

        self.identifier = identifier or URIRef(pathname2url(abspath(path)))


    def __len__(self, context=None):
        """
        Return length of the dataset.

        :param context: Context to restrict count to.
        :type context: rdflib.URIRef or rdflib.Graph
        """
        context = self._normalize_context(context)

        return self._len(context)


    # RDFLib DB management API

    def open(self, configuration=None, create=True):
        """
        Open the store environment.

        :param str configuration: If not specified on init, indicate the path
            to use for the store.
        :param bool create: Create the file and folder structure for the
            store environment.
        """
        if not self.is_open:
            logger.debug('Store is not open.')
            try:
                self.open_env(create)
            except:
                return NO_STORE
            self._open = True

        return VALID_STORE


    def close(self, commit_pending_transaction=False):
        """
        Close the database connection.

        Do this at server shutdown.
        """
        self.close_env(commit_pending_transaction)


    def destroy(self, path=''):
        """
        Destroy the store.

        https://www.youtube.com/watch?v=lIVq7FJnPwg

        :param str path: unused. Left for RDFLib API compatibility. (actually
            quite dangerous if it were used: it could turn into a
            general-purpose recursive file and folder delete method!)
        """
        return self._destroy()


    # RDFLib triple methods.

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

        # TODO: figure out how the RDFLib dispatcher is inherited
        # (and if there is a use for it in a first place)
        #Store.add(self, triple, context)

        pk_s, pk_p, pk_o = [self._pickle(t) for t in triple]
        pk_c = self._pickle(context)

        self._add(pk_s, pk_p, pk_o, pk_c)


    def remove(self, triple_pattern, context=None):
        """
        Remove triples by a pattern.

        :param tuple triple_pattern: 3-tuple of
        either RDF terms or None, indicating the triple(s) to be removed.
        ``None`` is used as a wildcard.
        :param context: Context to remove the triples from. If None (the
        default) the matching triples are removed from all contexts.
        :type context: rdflib.term.Identifier or None
        """
        #logger.debug('Removing triples by pattern: {} on context: {}'.format(
        #    triple_pattern, context))
        context = self._normalize_context(context)

        self._remove(triple_pattern, context)


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

        logger.debug(
                'Getting triples for: {}, {}'.format(triple_pattern, context))
        for spok in self.triple_keys(triple_pattern, context):
            logger.debug('spok: {}'.format(spok))
            if self.key_exists(spok, 'spo:c', new_txn=False):
                #logger.debug('preparing contexts.')
                contexts = tuple(
                    Graph(identifier=self.from_key(ck)[0], store=self)
                    for ck in self.get_dup_data(spok, 'spo:c'))
                logger.debug('contexts: {}'.format(contexts))
            else:
                contexts = (Graph(identifier=context),)

            logger.debug('Triple keys before yield: {}: {}.'.format(
                spok, contexts))
            yield self.from_key(spok), contexts
            logger.debug('After yield.')


    def bind(self, prefix, namespace):
        """
        Bind a prefix to a namespace.

        :param str prefix: Namespace prefix.
        :param rdflib.URIRef namespace: Fully qualified URI of namespace.
        """
        prefix = prefix.encode()
        namespace = namespace.encode()
        if self.is_txn_rw:
            self.put(prefix, namespace, 'pfx:ns')
            self.put(namespace, prefix, 'ns:pfx')
        else:
            logger.debug('Opening RW transaction.')
            with self.txn_ctx(write=True) as wtxn:
                self.put(prefix, namespace, 'pfx:ns')
                self.put(namespace, prefix, 'ns:pfx')


    def namespace(self, prefix):
        """
        Get the namespace for a prefix.
        :param str prefix: Namespace prefix.
        """
        ns = self.get_data(prefix.encode(), 'pfx:ns')

        return Namespace(ns.decode()) if ns is not None else None


    def prefix(self, namespace):
        """
        Get the prefix associated with a namespace.

        **Note:** A namespace can be only bound to one prefix in this
        implementation.

        :param rdflib.Namespace namespace: Fully qualified namespace.

        :rtype: str or None
        """
        prefix = self.get_data(str(namespace).encode(), 'ns:pfx')

        return prefix.decode() if prefix is not None else None


    def namespaces(self):
        """Get an iterator of all prefix: namespace bindings.

        :rtype: Iterator(tuple(str, rdflib.Namespace))
        """
        for pfx, ns in self.all_namespaces():
            yield (pfx, Namespace(ns))


    def contexts(self, triple=None):
        """
        Get a list of all contexts.

        :rtype: Iterator(rdflib.Graph)
        """
        for ctx_uri in self.all_contexts(triple):
            yield Graph(identifier=self.from_key(ctx_uri)[0], store=self)


    def add_graph(self, graph):
        """
        Add a graph to the database.

        This creates an empty graph by associating the graph URI with the
        pickled `None` value. This prevents from removing the graph when all
        triples are removed.

        This may be called by read-only operations:
        https://github.com/RDFLib/rdflib/blob/master/rdflib/graph.py#L1623
        In which case it needs to open a write transaction. This is not ideal
        but the only way to handle datasets in RDFLib.

        :param rdflib.URIRef graph: URI of the named graph to add.
        """
        if isinstance(graph, Graph):
            graph = graph.identifier
        pk_c = self._pickle(graph)
        self._add_graph(pk_c, len(pk_c))


    def remove_graph(self, graph):
        """
        Remove all triples from graph and the graph itself.

        :param rdflib.URIRef graph: URI of the named graph to remove.
        """
        if isinstance(graph, Graph):
            graph = graph.identifier
        self._remove_graph(graph)


    ## PRIVATE METHODS ##

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


    ## Convenience methods—not necessary for functioning but useful for
    ## debugging.

    #def _keys_in_ctx(self, pk_ctx):
    #    """
    #    Convenience method to list all keys in a context.

    #    :param bytes pk_ctx: Pickled context URI.

    #    :rtype: Iterator(tuple)
    #    :return: Generator of triples.
    #    """
    #    with self.cur('c:spo') as cur:
    #        if cur.set_key(pk_ctx):
    #            tkeys = cur.iternext_dup()
    #            return {self._key_to_triple(tk) for tk in tkeys}
    #        else:
    #            return set()


    #def _ctx_for_key(self, tkey):
    #    """
    #    Convenience method to list all contexts that a key is in.

    #    :param bytes tkey: Triple key.

    #    :rtype: Iterator(rdflib.URIRef)
    #    :return: Generator of context URIs.
    #    """
    #    with self.cur('spo:c') as cur:
    #        if cur.set_key(tkey):
    #            ctx = cur.iternext_dup()
    #            return {self._unpickle(c) for c in ctx}
    #        else:
    #            return set()
