import logging

from flask import Blueprint, current_app, request, render_template
from rdflib.plugin import PluginException

from lakesuperior.env import env
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.api import query as query_api
from lakesuperior.store.ldp_rs.lmdb_store import LmdbStore, TxnManager

# Query endpoint. raw SPARQL queries exposing the underlying layout can be made
# available. Also convenience methods that allow simple lookups based on simple
# binary comparisons should be added. Binary lookups—maybe?
# N.B All data sources are read-only for this endpoint.

logger = logging.getLogger(__name__)
rdf_store = env.app_globals.rdf_store
rdfly = env.app_globals.rdfly

query = Blueprint('query', __name__)


@query.route('/term_search', methods=['GET'])
def term_search():
    '''
    Search by entering a search term and optional property and comparison term.
    '''
    valid_operands = (
        ('=', 'Equals'),
        ('>', 'Greater Than'),
        ('<', 'Less Than'),
        ('<>', 'Not Equal'),
        ('a', 'RDF Type'),
    )

    term = request.args.get('term')
    prop = request.args.get('prop', default=1)
    cmp = request.args.get('cmp', default='=')

    return render_template('term_search.html')


@query.route('/sparql', methods=['GET', 'POST'])
def sparql():
    '''
    Perform a direct SPARQL query on the underlying triplestore.

    @param qry SPARQL query string.
    '''
    accept_mimetypes = {
        'text/csv': 'csv',
        'application/sparql-results+json': 'json',
        'application/sparql-results+xml': 'xml',
    }
    if request.method == 'GET':
        return render_template('sparql_query.html', nsm=nsm)
    else:
        if request.mimetype == 'multipart/form-data':
            qstr = request.form['query']
        else:
            qstr = stream.read()
        logger.debug('Query: {}'.format(qstr))
        with TxnManager(rdf_store) as txn:
            qres = query_api.sparql_query(qstr)

            match = request.accept_mimetypes.best_match(accept_mimetypes.keys())
            if match:
                enc = accept_mimetypes[match]
            else:
                enc = request.accept_mimetypes.best

            try:
                out = qres.serialize(format=enc)
            except PluginException:
                return (
                    'Unable to serialize results into format {}'.format(enc),
                    406)

    return out, 200
