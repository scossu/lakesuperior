import logging

from io import BytesIO

from lakesuperior import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore, TxnManager


logger = logging.getLogger(__name__)
rdfly = env.app_globals.rdfly
rdf_store = env.app_globals.rdf_store


def term_query(s=None, p=None, o=None):
    """
    Query store by matching triple patterns.

    Any of the ``s``, ``p`` or ``o`` terms can be None to represent a wildcard.

    This method is for triple matching only; it does not allow to query, nor
    exposes to the caller, any context.

    :param rdflib.term.Identifier s: Subject term.
    :param rdflib.term.Identifier p: Predicate term.
    :param rdflib.term.Identifier o: Object term.
    """
    with TxnManager(rdf_store) as txn:
        # Strip contexts and de-duplicate.
        qres = {match[0] for match in rdf_store.triples((s, p, o), None)}

    return qres


def lookup_literal(pattern):
    """
    Look up one literal term by partial match.

    *TODO: reserved for future use. A Whoosh or similar full-text index is
    necessary for this.*
    """
    pass


def sparql_query(qry_str, fmt):
    """
    Send a SPARQL query to the triplestore.

    :param str qry_str: SPARQL query string. SPARQL 1.1 Query Language
        (https://www.w3.org/TR/sparql11-query/) is supported.
    :param str fmt: Serialization format. This varies depending on the
        query type (SELECT, ASK, CONSTRUCT, etc.). [TODO Add reference to
        RDFLib serialization formats]

    :rtype: BytesIO
    :return: Serialized SPARQL results.
    """
    with TxnManager(rdf_store) as txn:
        qres = rdfly.raw_query(qry_str)
        out_stream = BytesIO(qres.serialize(format=fmt))

    return out_stream
