import hashlib
import logging
import os

from contextlib import ContextDecorator, ExitStack
from os import makedirs
from os.path import abspath
from urllib.request import pathname2url

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
        LmdbTriplestore.__init__(self, path, open_env=True, create=create)

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
            #logger.debug('Store is not open.')
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


    # RDFLib triple methods.

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


    def bind(self, prefix, namespace):
        """
        Bind a prefix to a namespace.

        :param str prefix: Namespace prefix.
        :param rdflib.URIRef namespace: Fully qualified URI of namespace.
        """
        prefix = prefix.encode()
        namespace = namespace.encode()
        if self.is_txn_rw:
            # FIXME DB labels should be constants but there are problems
            # imprting them from the Cython module.
            self.put(prefix, namespace, b'pfx:ns_')
            self.put(namespace, prefix, b'ns:pfx_')
        else:
            #logger.debug('Opening RW transaction.')
            with self.txn_ctx(write=True) as wtxn:
                self.put(prefix, namespace, b'pfx:ns_')
                self.put(namespace, prefix, b'ns:pfx_')


    def namespace(self, prefix):
        """
        Get the namespace for a prefix.
        :param str prefix: Namespace prefix.
        """
        ns = self.get_data(prefix.encode(), b'pfx:ns_')

        return Namespace(ns.decode()) if ns is not None else None


    def prefix(self, namespace):
        """
        Get the prefix associated with a namespace.

        **Note:** A namespace can be only bound to one prefix in this
        implementation.

        :param rdflib.Namespace namespace: Fully qualified namespace.

        :rtype: str or None
        """
        prefix = self.get_data(str(namespace).encode(), b'ns:pfx_')

        return prefix.decode() if prefix is not None else None


    def namespaces(self):
        """Get an iterator of all prefix: namespace bindings.

        :rtype: Iterator(tuple(str, rdflib.Namespace))
        """
        for pfx, ns in self.all_namespaces():
            yield (pfx, Namespace(ns))


    def remove_graph(self, graph):
        """
        Remove all triples from graph and the graph itself.

        :param rdflib.URIRef graph: URI of the named graph to remove.
        """
        if isinstance(graph, Graph):
            graph = graph.identifier
        self._remove_graph(graph)


    ## PRIVATE METHODS ##
