from flask import current_app, g

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm

class QueryEngine:
    '''
    Handle both simple term-based and full-fledged SPARQL queries.
    '''

    def sparql_query(self, qry):
        '''
        Send a SPARQL query to the triplestore.

        The returned value may be different.
        '''
        return current_app.rdfly.raw_query(qry)
