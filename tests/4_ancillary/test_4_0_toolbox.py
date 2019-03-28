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

    def test_uid_to_uri(self):
        assert g.tbox.uid_to_uri('/1234') == URIRef(g.webroot + '/1234')
        assert g.tbox.uid_to_uri('/1/2/34') == URIRef(g.webroot + '/1/2/34')
        assert g.tbox.uid_to_uri('') == URIRef(g.webroot)


    def test_uri_to_uid(self):
        assert g.tbox.uri_to_uid(URIRef(g.webroot) + '/test01') == '/test01'
        assert g.tbox.uri_to_uid(URIRef(g.webroot) + '/test01/test02') == \
                '/test01/test02'
        assert g.tbox.uri_to_uid(URIRef(g.webroot)) == '/'
        assert g.tbox.uri_to_uid(nsc['fcres']['/']) == '/'
        assert g.tbox.uri_to_uid(nsc['fcres']['/1234']) == '/1234'
        assert g.tbox.uri_to_uid(nsc['fcres']['/1234/5678']) == '/1234/5678'


    def test_localize_uri_string(self):
        '''
        Test string localization.
        '''
        assert g.tbox.localize_uri_string(g.webroot + '/test/uid') == \
                g.tbox.localize_uri_string(g.webroot + '/test/uid/') == \
                str(nsc['fcres']['/test/uid'])
        assert g.tbox.localize_uri_string(g.webroot) == str(nsc['fcres']['/'])
        assert g.tbox.localize_uri_string('http://bogus.org/test/uid') == \
                'http://bogus.org/test/uid'


    def test_localize_term(self):
        '''
        Test term localization.
        '''
        assert g.tbox.localize_term(g.webroot + '/test/uid') == \
                g.tbox.localize_term(g.webroot + '/test/uid/') == \
                nsc['fcres']['/test/uid']


    def test_localize_ext_str(self):
        '''
        Test extended string localization.
        '''
        input = '''
        @prefix bogus: <http://bogs.r.us#>
        @prefix ex: <{0}/ns#>

        FREE GRATUITOUS {{
          <#blah> a <{0}/type#A> .
          <> <info:ex/p> <xyz> .
          <#c#r#a#zy> ex:virtue <?blah#e> .
          <{0}> <{0}/go#lala>
            <{0}/heythere/gulp> .
        }} GARB AGE TOKENS {{
          <{0}/hey?there#hoho> <https://goglyeyes.com#cearch>
            <{0}#hehe> .
          <{0}?there#haha> <info:auth/blok> "Hi I'm a strong" .
        }}
        '''.format(g.webroot)
        exp_output = '''
        @prefix bogus: <http://bogs.r.us#>
        @prefix ex: <info:fcres/ns#>

        FREE GRATUITOUS {
          <info:fcres/123#blah> a <info:fcres/type#A> .
          <info:fcres/123> <info:ex/p> <xyz> .
          <info:fcres/123#c#r#a#zy> ex:virtue <info:fcres/123?blah#e> .
          <info:fcres/> <info:fcres/go#lala>
            <info:fcres/heythere/gulp> .
        } GARB AGE TOKENS {
          <info:fcres/hey?there#hoho> <https://goglyeyes.com#cearch>
            <info:fcres/#hehe> .
          <info:fcres/?there#haha> <info:auth/blok> "Hi I'm a strong" .
        }
        '''

        assert g.tbox.localize_ext_str(
                input, nsc['fcres']['/123']) == exp_output
