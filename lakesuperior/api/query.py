import logging

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.env import env


logger = logging.getLogger(__name__)
rdfly = env.app_globals.rdfly


def sparql_query(qry_str):
    '''
    Send a SPARQL query to the triplestore.

    @param qry_str (str) SPARQL query string. SPARQL 1.1 Query Language
    (https://www.w3.org/TR/sparql11-query/) is supported.

    @return rdflib.query.QueryResult
    '''
    return rdfly.raw_query(qry_str)
