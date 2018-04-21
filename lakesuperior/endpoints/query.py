import logging

from flask import Blueprint, current_app, request, render_template, send_file
from rdflib import URIRef
from rdflib.plugin import PluginException

from lakesuperior import env
from lakesuperior.api import query as query_api
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.toolbox import Toolbox

# Query endpoint. raw SPARQL queries exposing the underlying layout can be made
# available. Also convenience methods that allow simple lookups based on simple
# binary comparisons should be added. Binary lookups—maybe?
# N.B All data sources are read-only for this endpoint.

logger = logging.getLogger(__name__)
rdfly = env.app_globals.rdfly

query = Blueprint('query', __name__)


@query.route('/term_search', methods=['GET', 'POST'])
def term_search():
    """
    Search by entering a search term and optional property and comparison term.
    """
    operands = (
        ('_id', 'Has Type'),
        ('_id', 'Matches Term'),
        ('=', 'Is Equal To'),
        ('!=', 'Is Not Equal To'),
        ('<', 'Is Less Than'),
        ('>', 'Is Greater Than'),
        ('<=', 'Is Less Than Or Equal To'),
        ('>=', 'Is Greater Than Or Equal To'),
    )
    qres = term_list = []

    if request.method == 'POST':
        # Some magic needed to associate pseudo-array field notation with
        # an actual dict. Flask does not fully support this syntax as Rails
        # or other frameworks do: https://stackoverflow.com/q/24808660
        fnames = ('pred_ns', 'pred', 'op', 'val')
        term_list = [
                request.form.getlist('{}[]'.format(tn))
                for tn in fnames]
        # Transpose matrix.
        txm = list(zip(*term_list))
        logger.info('transposed matrix: {}'.format(txm))
        terms = []
        for row in txm:
            fmt_row = list(row)
            ns = fmt_row.pop(0)
            fmt_row[0] = nsc[ns][fmt_row[0]] if ns else URIRef(fmt_row[0])
            terms.append(fmt_row)
        logger.info('Terms: {}'.format(terms))

        or_logic = request.form.get('logic') == 'or'
        qres = query_api.term_query(terms, or_logic)

        def gl(uri):
            return uri.replace(nsc['fcres'], '/ldp')
        return render_template('term_search_results.html', qres=qres, gl=gl)
    else:
        return render_template(
            'term_search.html', operands=operands, qres=qres, nsm=nsm)


@query.route('/sparql', methods=['GET', 'POST'])
def sparql():
    """
    Perform a direct SPARQL query on the underlying triplestore.

    :param str qry: SPARQL query string.
    """
    accept_mimetypes = {
        'text/csv': 'csv',
        'application/sparql-results+json': 'json',
        'application/sparql-results+xml': 'xml',
    }
    if request.method == 'GET':
        return render_template('sparql_query.html', nsm=nsm)
    else:
        if request.mimetype == 'application/sparql-query':
            qstr = request.stream.read()
        else:
            qstr = request.form['query']
        logger.debug('Query: {}'.format(qstr))

        match = request.accept_mimetypes.best_match(accept_mimetypes.keys())
        fmt = (
                accept_mimetypes[match] if match
                else request.accept_mimetypes.best)

        try:
            out_stream = query_api.sparql_query(qstr, fmt)
        except PluginException:
            return (
                'Unable to serialize results into format {}'.format(fmt), 406)

    return send_file(out_stream, mimetype=fmt), 200
