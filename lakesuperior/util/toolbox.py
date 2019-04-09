import logging
import os
import re

from collections import defaultdict
from hashlib import sha1

from rdflib import Graph
from rdflib.term import URIRef, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.globals import ROOT_RSRC_URI


logger = logging.getLogger(__name__)

__doc__ = ''' Utility to translate and generate strings and other objects. '''


def fsize_fmt(num, suffix='b'):
    """
    Format an integer into 1024-block file size format.

    Adapted from Python 2 code on
    https://stackoverflow.com/a/1094933/3758232

    :param int num: Size value in bytes.
    :param str suffix: Suffix label (defaults to ``b``).

    :rtype: str
    :return: Formatted size to largest fitting unit.
    """
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return f'{num:3.1f} {unit}{suffix}'
        num /= 1024.0
    return f'{num:.1f} Y{suffix}'


def get_tree_size(path, follow_symlinks=True):
    """
    Return total size of files in given path and subdirs.

    Ripped from https://www.python.org/dev/peps/pep-0471/
    """
    total = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=follow_symlinks):
            total += get_tree_size(entry.path)
        else:
            total += entry.stat(
                follow_symlinks=follow_symlinks
            ).st_size

    return total


def replace_term_domain(term, search, replace):
    '''
    Replace the domain of a term.

    :param rdflib.URIRef term: The term (URI) to change.
    :param str search: Domain string to replace.
    :param str replace: Domain string to use for replacement.

    :rtype: rdflib.URIRef
    '''
    s = str(term)
    if s.startswith(search):
        s = s.replace(search, replace)

    return URIRef(s)


def parse_rfc7240(h_str):
    '''
    Parse ``Prefer`` header as per https://tools.ietf.org/html/rfc7240

    The ``cgi.parse_header`` standard method does not work with all
    possible use cases for this header.

    :param str h_str: The header(s) as a comma-separated list of Prefer
        statements, excluding the ``Prefer:`` token.
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


def split_uuid(uuid):
    '''
    Split a UID into pairtree segments. This mimics FCREPO4 behavior.

    :param str uuid: UUID to split.

    :rtype: str
    '''
    path = '{}/{}/{}/{}/{}'.format(uuid[:2], uuid[2:4],
            uuid[4:6], uuid[6:8], uuid)

    return path



class RequestUtils:
    """
    Utilities that require access to an HTTP request context.

    Initialize this within a Flask request context.
    """
    def __init__(self):
        from flask import g
        self.webroot = g.webroot


    def uid_to_uri(self, uid):
        '''Convert a UID to a URI.

        :rtype: rdflib.URIRef
        '''
        return URIRef(self.webroot + uid)


    def uri_to_uid(self, uri):
        '''Convert an absolute URI (internal or external) to a UID.

        :rtype: str
        '''
        if uri.startswith(nsc['fcres']):
            return str(uri).replace(nsc['fcres'], '')
        else:
            return '/' + str(uri).replace(self.webroot, '').strip('/')


    def localize_uri_string(self, s):
        '''Convert URIs into URNs in a string using the application base URI.

        :param str: s Input string.

        :rtype: str
        '''
        if s.strip('/') == self.webroot:
            return str(ROOT_RSRC_URI)
        else:
            return s.rstrip('/').replace(
                    self.webroot, str(nsc['fcres']))


    def localize_term(self, uri):
        '''
        Localize an individual term.

        :param rdflib.URIRef: urn Input URI.

        :rtype: rdflib.URIRef
        '''
        return URIRef(self.localize_uri_string(str(uri)))


    def localize_triple(self, trp):
        '''
        Localize terms in a triple.

        :param tuple(rdflib.URIRef) trp: The triple to be converted

        :rtype: tuple(rdflib.URIRef)
        '''
        s, p, o = trp
        if s.startswith(self.webroot):
            s = self.localize_term(s)
        if o.startswith(self.webroot):
            o = self.localize_term(o)

        return s, p, o


    def localize_graph(self, gr):
        '''
        Localize a graph.
        '''
        l_id = self.localize_term(gr.identifier)
        l_gr = Graph(identifier=l_id)
        for trp in gr:
            l_gr.add(self.localize_triple(trp))

        return l_gr


    def localize_payload(self, data):
        '''
        Localize an RDF stream with domain-specific URIs.

        :param bytes data: Binary RDF data.

        :rtype: bytes
        '''
        return data.replace(
            (self.webroot + '/').encode('utf-8'),
            (nsc['fcres'] + '/').encode('utf-8')
        ).replace(
            self.webroot.encode('utf-8'),
            (nsc['fcres'] + '/').encode('utf-8')
        )


    def localize_ext_str(self, s, urn):
        '''
        Convert global URIs to local in a SPARQL or RDF string.

        Also replace empty URIs (`<>`) with a fixed local URN and take care
        of fragments and relative URIs.

        This is a 3-pass replacement. First, global URIs whose webroot matches
        the application ones are replaced with internal URIs. Then, relative
        URIs are converted to absolute using the internal URI as the base;
        finally, the root node is appropriately addressed.
        '''
        esc_webroot = self.webroot.replace('/', '\\/')
        #loc_ptn = r'<({}\/?)?(.*?)?(\?.*?)?(#.*?)?>'.format(esc_webroot)
        loc_ptn1 = r'<{}\/?(.*?)>'.format(esc_webroot)
        loc_sub1 = '<{}/\\1>'.format(nsc['fcres'])
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

        :param string s: Input string.

        :rtype: string
        '''
        return s.replace(str(nsc['fcres']), self.webroot)


    def globalize_term(self, urn):
        '''
        Convert an URN into an URI using the application base URI.

        :param rdflib.URIRef urn: Input URN.

        :rtype: rdflib.URIRef
        '''
        return URIRef(self.globalize_string(str(urn)))


    def globalize_triple(self, trp):
        '''
        Globalize terms in a triple.

        :param tuple(rdflib.URIRef) trp: The triple to be converted

        :rtype: tuple(rdflib.URIRef)
        '''
        s, p, o = trp
        if s.startswith(nsc['fcres']):
            s = self.globalize_term(s)
        if o.startswith(nsc['fcres']):
            o = self.globalize_term(o)

        return s, p, o


    def globalize_imr(self, imr):
        '''
        Globalize an Imr.

        :rtype: rdflib.Graph
        '''
        g_gr = Graph(identifier=self.globalize_term(imr.uri))
        for trp in imr:
            g_gr.add(self.globalize_triple(trp))

        return g_gr


    def globalize_graph(self, gr):
        '''
        Globalize a graph.
        '''
        g_id = self.globalize_term(gr.identifier)
        g_gr = Graph(identifier=g_id)
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

