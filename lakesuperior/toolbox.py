import logging
import pickle
import re

from collections import defaultdict
from hashlib import sha1

from flask import g
from rdflib import Graph
from rdflib.term import URIRef, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import ROOT_RSRC_URI


class Toolbox:
    '''
    Utility class to translate and generate strings and other objects.
    '''

    _logger = logging.getLogger(__name__)

    def replace_term_domain(self, term, search, replace):
        '''
        Replace the domain of a term.

        @param term (URIRef) The term (URI) to change.
        @param search (string) Domain string to replace.
        @param replace (string) Domain string to use for replacement.

        @return URIRef
        '''
        s = str(term)
        if s.startswith(search):
            s = s.replace(search, replace)

        return URIRef(s)


    def uuid_to_uri(self, uid):
        '''Convert a UID to a URI.

        @return URIRef
        '''
        uri = '{}/{}'.format(g.webroot, uid) if uid else g.webroot

        return URIRef(uri)


    def uri_to_uuid(self, uri):
        '''Convert an absolute URI (internal or external) to a UID.

        @return string
        '''
        if uri.startswith(nsc['fcres']):
            return str(uri).replace(nsc['fcres'], '')
        else:
            return str(uri).replace(g.webroot, '').strip('/')


    def localize_string(self, s):
        '''Convert URIs into URNs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        if s.strip('/') == g.webroot:
            return str(ROOT_RSRC_URI)
        else:
            return s.strip('/').replace(g.webroot+'/', str(nsc['fcres']))


    def localize_term(self, uri):
        '''
        Localize an individual term.

        @param rdflib.term.URIRef urn Input URI.

        @return rdflib.term.URIRef
        '''
        return URIRef(self.localize_string(str(uri)))


    def localize_triple(self, trp):
        '''
        Localize terms in a triple.

        @param trp (tuple(rdflib.term.URIRef)) The triple to be converted

        @return tuple(rdflib.term.URIRef)
        '''
        s, p, o = trp
        if s.startswith(g.webroot):
            s = self.localize_term(s)
        if o.startswith(g.webroot):
            o = self.localize_term(o)

        return s, p, o


    def localize_graph(self, gr):
        '''
        Localize a graph.
        '''
        l_gr = Graph()
        for trp in gr:
            l_gr.add(self.localize_triple(trp))

        return l_gr


    def localize_ext_str(self, s, urn):
        '''
        Convert global URIs to local in a SPARQL or RDF string.

        Also replace empty URIs (`<>`) with a fixed local URN and take care
        of fragments and relative URIs.

        This is a 3-pass replacement. First, global URIs whose webroot matches
        the application ones are replaced with local URNs. Then, relative URIs
        are converted to absolute using the URN as the base; finally, the
        root node is appropriately addressed.
        '''
        esc_webroot = g.webroot.replace('/', '\\/')
        #loc_ptn = r'<({}\/?)?(.*?)?(\?.*?)?(#.*?)?>'.format(esc_webroot)
        loc_ptn1 = r'<{}\/?(.*?)>'.format(esc_webroot)
        loc_sub1 = '<{}\\1>'.format(nsc['fcres'])
        s1 = re.sub(loc_ptn1, loc_sub1, s)

        loc_ptn2 = r'<([#?].*?)?>'
        loc_sub2 = '<{}\\1>'.format(urn)
        s2 = re.sub(loc_ptn2, loc_sub2, s1)

        loc_ptn3 = r'<{}([#?].*?)?>'.format(nsc['fcres'])
        loc_sub3 = '<{}\\1>'.format(ROOT_RSRC_URI)
        s3 = re.sub(loc_ptn3, loc_sub3, s2)

        return s3


    def globalize_string(self, s):
        '''Convert URNs into URIs in a string using the application base URI.

        @param string s Input string.

        @return string
        '''
        return s.replace(str(nsc['fcres']), g.webroot + '/')


    def globalize_term(self, urn):
        '''
        Convert an URN into an URI using the application base URI.

        @param rdflib.term.URIRef urn Input URN.

        @return rdflib.term.URIRef
        '''
        if urn == ROOT_RSRC_URI:
            urn = nsc['fcres']

        return URIRef(self.globalize_string(str(urn)))


    def globalize_triple(self, trp):
        '''
        Globalize terms in a triple.

        @param trp (tuple(rdflib.term.URIRef)) The triple to be converted

        @return tuple(rdflib.term.URIRef)
        '''
        s, p, o = trp
        if s.startswith(nsc['fcres']):
            s = self.globalize_term(s)
        if o.startswith(nsc['fcres']):
            o = self.globalize_term(o)

        return s, p, o


    def globalize_graph(self, gr):
        '''
        Globalize a graph.
        '''
        g_gr = Graph()
        for trp in gr:
            g_gr.add(self.globalize_triple(trp))

        return g_gr


    def globalize_rsrc(self, rsrc):
        '''
        Globalize a resource.
        '''
        gr = rsrc.graph
        urn = rsrc.identifier

        global_gr = self.globalize_graph(gr)
        global_uri = self.globalize_term(urn)

        return global_gr.resource(global_uri)


    def parse_rfc7240(self, h_str):
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
                param_parts = [ prm.strip().strip('"') \
                        for prm in param_token.split('=') ]
                param_value = param_parts[1] if len(param_parts) > 1 else None
                parsed_pref['parameters'][param_parts[0]] = param_value

            parsed_hdr[prefer_name] = parsed_pref

        return parsed_hdr


    def rdf_cksum(self, gr):
        '''
        Generate a checksum for a graph.

        This is not straightforward because a graph is derived from an
        unordered data structure (RDF).

        What this method does is ordering the graph by subject, predicate,
        object, then creating a pickle string and a checksum of it.

        N.B. The context of the triples is ignored, so isomorphic graphs would
        have the same checksum regardless of the context(s) they are found in.

        @TODO This can be later reworked to use a custom hashing algorithm.

        @param rdflib.Graph gr The graph to be hashed.

        @return string SHA1 checksum.
        '''
        # Remove the messageDigest property, which very likely reflects the
        # previous state of the resource.
        gr.remove((Variable('s'), nsc['premis'].messageDigest, Variable('o')))

        ord_gr = sorted(list(gr), key=lambda x : (x[0], x[1], x[2]))
        hash = sha1(pickle.dumps(ord_gr)).hexdigest()

        return hash

    def split_uuid(self, uuid):
        '''
        Split a UID into pairtree segments. This mimics FCREPO4 behavior.
        '''
        path = '{}/{}/{}/{}/{}'.format(uuid[:2], uuid[2:4],
                uuid[4:6], uuid[6:8], uuid)

        return path
