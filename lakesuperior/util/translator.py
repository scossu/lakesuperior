import logging

from collections import defaultdict

from flask import request, g
from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout


class Translator:
    '''
    Utility class to perform translations of strings and their wrappers.
    All static methods.
    '''

    _logger = logging.getLogger(__name__)

    @staticmethod
    def base_url():
        return request.host_url + g.url_prefix


    @staticmethod
    def camelcase(word):
        '''
        Convert a string with underscores with a camel-cased one.

        Ripped from https://stackoverflow.com/a/6425628
        '''
        return ''.join(x.capitalize() or '_' for x in word.split('_'))


    @staticmethod
    def uuid_to_uri(uuid):
        '''Convert a UUID to a URI.

        @return URIRef
        '''
        return URIRef('{}/{}'.format(Translator.base_url(), uuid))


    @staticmethod
    def uri_to_uuid(uri):
        '''Convert an absolute URI (internal or external) to a UUID.

        @return string
        '''
        if uri.startswith(nsc['fcres']):
            return str(uri).replace(nsc['fcres'], '')
        else:
            return str(uri).replace(Translator.base_url(), '')


    @staticmethod
    def localize_string(s):
        '''Convert URIs into URNs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(Translator.base_url()+'/', str(nsc['fcres']))\
                .replace(Translator.base_url(), str(nsc['fcres']))


    @staticmethod
    def localize_term(uri):
        '''
        Convert an URI into an URN.

        @param rdflib.term.URIRef urn Input URI.

        @return rdflib.term.URIRef
        '''
        Translator._logger.debug('Input URI: {}'.format(uri))
        if uri.strip('/') == Translator.base_url():
            return BaseRdfLayout.ROOT_NODE_URN
        return URIRef(Translator.localize_string(str(uri)))


    @staticmethod
    def globalize_string(s):
        '''Convert URNs into URIs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(str(nsc['fcres']), Translator.base_url() + '/')


    @staticmethod
    def globalize_term(urn):
        '''
        Convert an URN into an URI using the application base URI.

        @param rdflib.term.URIRef urn Input URN.

        @return rdflib.term.URIRef
        '''
        if urn == BaseRdfLayout.ROOT_NODE_URN:
            urn = nsc['fcres']
        return URIRef(Translator.globalize_string(str(urn)))


    @staticmethod
    def globalize_graph(g):
        '''
        Globalize a graph.
        '''
        from lakesuperior.model.ldpr import Ldpr
        q = '''
        CONSTRUCT {{ ?s ?p ?o . }} WHERE {{
          {{
            ?s ?p ?o .
            FILTER (
              STRSTARTS(str(?s), "{0}")
              ||
              STRSTARTS(str(?o), "{0}")
              ||
              STRSTARTS(str(?s), "{1}")
              ||
              STRSTARTS(str(?o), "{1}")
            ) .
          }}
        }}'''.format(nsc['fcres'], BaseRdfLayout.ROOT_NODE_URN)
        flt_g = g.query(q)

        for t in flt_g:
            global_s = Translator.globalize_term(t[0])
            global_o = Translator.globalize_term(t[2]) \
                    if isinstance(t[2], URIRef) \
                    else t[2]
            g.remove(t)
            g.add((global_s, t[1], global_o))

        return g


    @staticmethod
    def globalize_rsrc(rsrc):
        '''
        Globalize a resource.
        '''
        g = rsrc.graph
        urn = rsrc.identifier

        global_g = Translator.globalize_graph(g)
        global_uri = Translator.globalize_term(urn)

        return global_g.resource(global_uri)


    @staticmethod
    def parse_rfc7240(h_str):
        '''
        Parse `Prefer` header as per https://tools.ietf.org/html/rfc7240

        The `cgi.parse_header` standard method does not work with all possible
        use cases for this header.

        @param h_str (string) The header(s) as a comma-separated list of Prefer
        statements, excluding the `Prefer: ` token.
        '''
        parsed_hdr = defaultdict(dict)

        # Split up headers by comma
        hdr_list = [ x.strip() for x in h_str.split(',') ]
        for hdr in hdr_list:
            parsed_pref = defaultdict(dict)
            # Split up tokens by semicolon
            token_list = [ token.strip() for token in hdr.split(';') ]
            prefer_token = token_list.pop(0).split('=')
            prefer_name = prefer_token[0]
            # If preference has a '=', it has a value, else none.
            if len(prefer_token)>1:
                parsed_pref['value'] = prefer_token[1].strip('"')

            for param_token in token_list:
                # If the token list had a ';' the preference has a parameter.
                print('Param token: {}'.format(param_token))
                param_parts = [ prm.strip().strip('"') \
                        for prm in param_token.split('=') ]
                param_value = param_parts[1] if len(param_parts) > 1 else None
                parsed_pref['parameters'][param_parts[0]] = param_value

            parsed_hdr[prefer_name] = parsed_pref

        return parsed_hdr


