from flask import Blueprint, request

# Query endpoint. raw SPARQL queries exposing the underlying layout can be made
# available. Also convenience methods that allow simple lookups based on simple
# binary comparisons should be added. Binary lookups—maybe?
# N.B All data sources are read-only for this endpoint.


query = Blueprint('query', __name__)


@query.route('/find', methods=['GET'])
def find():
    '''
    Search by entering a search term and optional property and comparison term.
    '''
    valid_operands = ('=', '>', '<', '<>')

    term = request.args.get('term')
    prop = request.args.get('prop', default=1)
    cmp = request.args.get('cmp', default='=')
    # @TODO


@query.route('/sparql', methods=['POST'])
def sparql(q):
    '''
    Perform a direct SPARQL query on the underlying triplestore.

    @param q SPARQL query string.
    '''
    # @TODO
    pass
