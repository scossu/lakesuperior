import logging

from io import BytesIO

from lakesuperior import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore, TxnManager


logger = logging.getLogger(__name__)
rdfly = env.app_globals.rdfly
rdf_store = env.app_globals.rdf_store


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
