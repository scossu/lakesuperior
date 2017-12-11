import pytest

from flask import g
from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.toolbox import Toolbox


@pytest.fixture
def tb(client):
    c = client.get('/ldp')
    return Toolbox()



class TestToolbox:
    '''
    Unit tests for toolbox methods.
    '''
    #def test_camelcase(self, tb):
    #    '''
    #    Test conversion from underscore notation to camelcase.
    #    '''
    #    assert tb.camelcase('test_input_string') == 'TestInputString'
    #    assert tb.camelcase('_test_input_string') == '_TestInputString'
    #    assert tb.camelcase('test__input__string') == 'Test_Input_String'


    def test_uuid_to_uri(self, tb):
        assert tb.uuid_to_uri('1234') == URIRef(g.webroot + '/1234')
        assert tb.uuid_to_uri('') == URIRef(g.webroot)


    def test_uri_to_uuid(self, tb):
        assert tb.uri_to_uuid(URIRef(g.webroot) + '/test01') == 'test01'
        assert tb.uri_to_uuid(URIRef(g.webroot) + '/test01/test02') == \
                'test01/test02'
        assert tb.uri_to_uuid(URIRef(g.webroot)) == ''
        assert tb.uri_to_uuid(nsc['fcsystem'].root) == None
        assert tb.uri_to_uuid(nsc['fcres']['1234']) == '1234'
        assert tb.uri_to_uuid(nsc['fcres']['1234/5678']) == '1234/5678'


    def test_localize_string(self, tb):
        '''
        Test string localization.
        '''
        assert tb.localize_string(g.webroot + '/test/uid') == \
                tb.localize_string(g.webroot + '/test/uid/') == \
                str(nsc['fcres']['test/uid'])
        assert tb.localize_string(g.webroot) == str(nsc['fcsystem'].root)
        assert tb.localize_string('http://bogus.org/test/uid') == \
                'http://bogus.org/test/uid'


    def test_localize_term(self, tb):
        '''
        Test term localization.
        '''
        assert tb.localize_term(g.webroot + '/test/uid') == \
                tb.localize_term(g.webroot + '/test/uid/') == \
                nsc['fcres']['test/uid']
