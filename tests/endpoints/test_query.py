import json
import pdb
import pytest

from flask import g

from lakesuperior.dictionaries.namespaces import ns_collection as nsc

@pytest.mark.usefixtures('client_class')
@pytest.mark.usefixtures('db')
class TestTermSearch:
    """
    Test term search endpoint.
    """

    def test_query_all(self):
        """
        Query all LDP resources.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [{
                    'pred': 'rdf:type',
                    'op': '_id',
                    'val': 'ldp:Resource',
                }],
            }), content_type='application/json')

        assert g.webroot + '/' in rsp.json


    def test_query_non_root(self):
        """
        Query non-root resources.
        """
        put_resp = self.client.put('/ldp/test_term_search',
            data=b'<> <http://www.w3.org/2004/02/skos/core#prefLabel> "Hello" .',
            content_type='text/turtle')
        assert put_resp.status_code == 201
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [{
                    'pred': 'skos:prefLabel',
                    'op': '_id',
                    'val': '"Hello"',
                }],
            }), content_type='application/json')

        assert rsp.json == [g.webroot + '/test_term_search']


    def test_query_root(self):
        """
        Query root.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [{
                    'pred': 'rdf:type',
                    'op': '_id',
                    'val': 'fcrepo:RepositoryRoot',
                }],
            }), content_type='application/json')

        assert rsp.json == [g.webroot + '/']


    def test_query_string_eq(self):
        """
        Query by string-wise equality.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [{
                    'pred': 'skos:prefLabel',
                    'op': '=',
                    'val': 'Hello',
                }],
            }), content_type='application/json')

        assert rsp.json == [g.webroot + '/test_term_search']


    def test_query_string_neq(self):
        """
        Query by string-wise inequality.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [{
                    'pred': 'skos:prefLabel',
                    'op': '!=',
                    'val': 'Repository Root',
                }],
            }), content_type='application/json')

        assert rsp.json == [g.webroot + '/test_term_search']


    def test_query_fquri(self):
        """
        Query using fully qualified URIs.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [{
                    'pred': '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ',
                    'op': '_id',
                    'val': '<http://fedora.info/definitions/v4/repository#RepositoryRoot>',
                }],
            }), content_type='application/json')

        assert rsp.json == [g.webroot + '/']


    def test_query_multi_term_and(self):
        """
        Query using two terms and AND logic.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'and',
                'terms': [
                    {
                        'pred': 'rdf:type',
                        'op': '_id',
                        'val': 'ldp:Container',
                    },
                    {
                        'pred': 'skos:prefLabel',
                        'op': '=',
                        'val': 'Hello',
                    },
                ],
            }), content_type='application/json')

        assert rsp.json == [g.webroot + '/test_term_search']


    def test_query_multi_term_or(self):
        """
        Query using two terms and AND logic.
        """
        self.client.get('/ldp')
        rsp = self.client.post(
            '/query/term_search', data=json.dumps({
                'logic': 'or',
                'terms': [
                    {
                        'pred': 'rdf:type',
                        'op': '_id',
                        'val': 'ldp:Container',
                    },
                    {
                        'pred': 'skos:prefLabel',
                        'op': '=',
                        'val': 'Hello',
                    },
                ],
            }), content_type='application/json')

        assert g.webroot + '/' in rsp.json
        assert g.webroot + '/test_term_search' in rsp.json
