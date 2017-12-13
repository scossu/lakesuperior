import pytest

from flask import g
from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc

@pytest.fixture
def app_ctx(client):
    '''
    Initialize the app context.
    '''
    return client.head('/ldp')


@pytest.mark.usefixtures('app_ctx')
class TestToolbox:
    '''
    Unit tests for toolbox methods.
    '''
    #def test_camelcase(self):
    #    '''
    #    Test conversion from underscore notation to camelcase.
    #    '''
    #    assert g.tbox.camelcase('test_input_string') == 'TestInputString'
    #    assert g.tbox.camelcase('_test_input_string') == '_TestInputString'
    #    assert g.tbox.camelcase('test__input__string') == 'Test_Input_String'

    def test_uuid_to_uri(self):
        assert g.tbox.uuid_to_uri('1234') == URIRef(g.webroot + '/1234')
        assert g.tbox.uuid_to_uri('') == URIRef(g.webroot)


    def test_uri_to_uuid(self):
        assert g.tbox.uri_to_uuid(URIRef(g.webroot) + '/test01') == 'test01'
        assert g.tbox.uri_to_uuid(URIRef(g.webroot) + '/test01/test02') == \
                'test01/test02'
        assert g.tbox.uri_to_uuid(URIRef(g.webroot)) == ''
        assert g.tbox.uri_to_uuid(nsc['fcsystem'].root) == None
        assert g.tbox.uri_to_uuid(nsc['fcres']['1234']) == '1234'
        assert g.tbox.uri_to_uuid(nsc['fcres']['1234/5678']) == '1234/5678'


    def test_localize_string(self):
        '''
        Test string localization.
        '''
        assert g.tbox.localize_string(g.webroot + '/test/uid') == \
                g.tbox.localize_string(g.webroot + '/test/uid/') == \
                str(nsc['fcres']['test/uid'])
        assert g.tbox.localize_string(g.webroot) == str(nsc['fcsystem'].root)
        assert g.tbox.localize_string('http://bogus.org/test/uid') == \
                'http://bogus.org/test/uid'


    def test_localize_term(self):
        '''
        Test term localization.
        '''
        assert g.tbox.localize_term(g.webroot + '/test/uid') == \
                g.tbox.localize_term(g.webroot + '/test/uid/') == \
                nsc['fcres']['test/uid']
