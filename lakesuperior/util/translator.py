from flask import request
from rdflib.term import URIRef

from lakesuperior.core.namespaces import ns_collection as nsc


class Translator:
    '''
    Utility class to perform translations of strings and their wrappers.
    All static methods.
    '''

    @staticmethod
    def uuid_to_uri(uuid):
        '''Convert a UUID to a URI.

        @return URIRef
        '''
        return URIRef('{}rest/{}'.format(request.host_url, uuid))


    @staticmethod
    def localize_string(s):
        '''Convert URIs into URNs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(
            request.host_url + 'rest/',
            str(nsc['fcres'])
        )


    @staticmethod
    def globalize_string(s):
        '''Convert URNs into URIs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(
            str(nsc['fcres']),
            request.host_url + 'rest/'
        )


    @staticmethod
    def globalize_term(urn):
        '''
        Convert an URN into an URI using the application base URI.

        @param rdflib.term.URIRef urn Input URN.

        @return rdflib.term.URIRef
        '''
        return URIRef(Translator.globalize_string(str(urn)))


    @staticmethod
    def globalize_graph(g):
        '''
        Globalize a graph.
        '''
        q = '''
        CONSTRUCT {{ ?s ?p ?o . }} WHERE {{
          ?s ?p ?o .
          {{ FILTER STRSTARTS(str(?s), "{0}") . }}
          UNION
          {{ FILTER STRSTARTS(str(?o), "{0}") . }}
        }}'''.format(nsc['fcres'])
        flt_g = g.query(q)

        for t in flt_g:
            print('Triple: {}'.format(t))
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
