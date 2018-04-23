import logging

from flask import (
        Blueprint, current_app, jsonify, request, make_response,
        render_template, send_file)
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
        terms = request.json.get('terms', {})
        or_logic = request.json.get('logic', 'and') == 'or'
        logger.info('Form: {}'.format(request.json))
        logger.info('Terms: {}'.format(terms))
        logger.info('Logic: {}'.format(or_logic))
        qres = query_api.term_query(terms, or_logic)

        rsp = [
            uri.replace(nsc['fcres'], request.host_url.rstrip('/') + '/ldp')
            for uri in qres]
        return jsonify(rsp), 200
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
