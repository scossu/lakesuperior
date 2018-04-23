import logging

from io import BytesIO

from rdflib import URIRef

from lakesuperior import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore, TxnManager


logger = logging.getLogger(__name__)
rdfly = env.app_globals.rdfly
rdf_store = env.app_globals.rdf_store

operands = ('_id', '=', '!=', '<', '>', '<=', '>=')
"""
Available term comparators for term query.

The ``_uri`` term is used to match URIRef terms, all other comparators are
used against literals.
"""


def triple_match(s=None, p=None, o=None, return_full=False):
    """
    Query store by matching triple patterns.

    Any of the ``s``, ``p`` or ``o`` terms can be None to represent a wildcard.

    This method is for triple matching only; it does not allow to query, nor
    exposes to the caller, any context.

    :param rdflib.term.Identifier s: Subject term.
    :param rdflib.term.Identifier p: Predicate term.
    :param rdflib.term.Identifier o: Object term.
    :param bool return_full: if ``False`` (the default), the returned values
        in the set are the URIs of the resources found. If True, the full set
        of matching triples is returned.

    :rtype: set(tuple(rdflib.term.Identifier){3}) or set(rdflib.URIRef)
    :return: Matching resource URIs if ``return_full`` is false, or
        matching triples otherwise.
    """
    with TxnManager(rdf_store) as txn:
        matches = rdf_store.triples((s, p, o), None)
        # Strip contexts and de-duplicate.
        qres = (
            {match[0] for match in matches} if return_full
            else {match[0][0] for match in matches})

    return qres


def term_query(terms, or_logic=False):
    """
    Query resources by predicates, comparators and values.

    Comparators can be against literal or URIRef objects. For a list of
    comparators and their meanings, see the documentation and source for
    :py:data:`~lakesuperior.api.query.operands`.

    :param list(tuple{3}) terms: List of 3-tuples containing:

        - Predicate URI (rdflib.URIRef)
        - Comparator value (str)
        - Value to compare to (rdflib.URIRef or rdflib.Literal or str)

    :param bool or_logic: Whether to concatenate multiple query terms with OR
        logic (uses SPARQL ``UNION`` statements). The default is False (i.e.
        terms are concatenated as standard SPARQL statements).
    """
    qry_term_ls = []
    for i, term in enumerate(terms):
        if term['op'] not in operands:
            raise ValueError('Not a valid operand: {}'.format(term['op']))

        if term['op'] == '_id':
            qry_term = '?s {} {} .'.format(term['pred'], term['val'])
        else:
            oname = '?o_{}'.format(i)
            qry_term = '?s {0} {1}\nFILTER (str({1}) {2} "{3}") .'.format(
                    term['pred'], oname, term['op'], term['val'])

        qry_term_ls.append(qry_term)

    if or_logic:
        qry_terms = '{\n' + '\n} UNION {\n'.join(qry_term_ls) + '\n}'
    else:
        qry_terms = '\n'.join(qry_term_ls)
    qry_str = '''
    SELECT ?s WHERE {{
      {}
    }}
    '''.format(qry_terms)
    logger.debug('Query: {}'.format(qry_str))

    with TxnManager(rdf_store) as txn:
        qres = rdfly.raw_query(qry_str)
        return {row[0] for row in qres}


def fulltext_lookup(pattern):
    """
    Look up one term by partial match.

    *TODO: reserved for future use. A `Whoosh
    <https://whoosh.readthedocs.io/>`__ or similar full-text index is
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
