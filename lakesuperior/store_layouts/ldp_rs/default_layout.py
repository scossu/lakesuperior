from copy import deepcopy
from pprint import pformat

from flask import current_app, g, request
from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import Literal, URIRef, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.dictionaries.srv_mgd_terms import (srv_mgd_subjects,
        srv_mgd_predicates, srv_mgd_types)
from lakesuperior.exceptions import (InvalidResourceError, InvalidTripleError,
        ResourceNotExistsError, TombstoneError)
from lakesuperior.store_layouts.ldp_rs.base_rdf_layout import BaseRdfLayout


class DefaultLayout(BaseRdfLayout):
    '''
    This is the default layout.

    Main triples are stored in a `main` graph; metadata in the `meta` graph;
    and historic snapshots (versions) in `historic`.
    '''

    HIST_GRAPH_URI = nsc['fcg'].historic
    MAIN_GRAPH_URI = nsc['fcg'].main
    META_GRAPH_URI = nsc['fcg'].metadata


    def extract_imr(self, uri, strict=True, incl_inbound=False,
                incl_children=True, embed_children=False, incl_srv_mgd=True):
        '''
        See base_rdf_layout.extract_imr.
        '''
        inbound_construct = '\n?s1 ?p1 ?s .' if incl_inbound else ''
        inbound_qry = '\nOPTIONAL { ?s1 ?p1 ?s . } .' if incl_inbound else ''

        # Include and/or embed children.
        embed_children_trp = embed_children_qry = ''
        if incl_srv_mgd and incl_children:
            incl_children_qry = ''

            # Embed children.
            if embed_children:
                embed_children_trp = '?c ?cp ?co .'
                embed_children_qry = '''
                OPTIONAL {{
                  ?s ldp:contains ?c .
                  {}
                }}
                '''.format(embed_children_trp)
        else:
            incl_children_qry = '\nFILTER ( ?p != ldp:contains )' \

        q = '''
        CONSTRUCT {{
            ?s ?p ?o .{inb_cnst}
            {embed_chld_t}
            ?s fcrepo:writable true ;
              fcrepo:hasParent ?parent .
        }} WHERE {{
            GRAPH ?main_graph {{
              ?s ?p ?o .{inb_qry}{incl_chld}{embed_chld}
              OPTIONAL {{
                ?parent ldp:contains ?s .
              }}
            }}
        }}
        '''.format(inb_cnst=inbound_construct,
                inb_qry=inbound_qry, incl_chld=incl_children_qry,
                embed_chld_t=embed_children_trp, embed_chld=embed_children_qry)

        try:
            qres = self._conn.query(q, initBindings={
                's': uri, 'main_graph': self.MAIN_GRAPH_URI})
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            gr = Graph()
        else:
            gr = qres.graph

        #self._logger.debug('Found resource: {}'.format(
        #        gr.serialize(format='turtle').decode('utf-8')))
        if strict and not len(gr):
            raise ResourceNotExistsError(uri)

        rsrc = Resource(gr, uri)

        # Check if resource is a tombstone.
        if rsrc[RDF.type : nsc['fcsystem'].Tombstone]:
            if strict:
                raise TombstoneError(
                        g.tbox.uri_to_uuid(rsrc.identifier),
                        rsrc.value(nsc['fcrepo'].created))
            else:
                self._logger.info('No resource found: {}'.format(uri))
        elif rsrc.value(nsc['fcsystem'].tombstone):
            if strict:
                raise TombstoneError(
                        g.tbox.uri_to_uuid(
                            rsrc.value(nsc['fcsystem'].tombstone).identifier),
                        rsrc.value(nsc['fcrepo'].created))
            else:
                self._logger.info('Tombstone found: {}'.format(uri))

        return rsrc


    def ask_rsrc_exists(self, urn):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        self._logger.info('Checking if resource exists: {}'.format(urn))

        return bool(self._conn.query(
                'ASK { GRAPH ?g { ?s ?p ?o . }}', initBindings={
                    's': urn, 'g': self.MAIN_GRAPH_URI}))


    def get_version_info(self, urn):
        '''
        See base_rdf_layout.get_version_info.
        '''
        q = '''
        CONSTRUCT {
          ?s fcrepo:hasVersion ?v .
          ?v ?p ?o .
        } WHERE {
          GRAPH fcg:metadata {
            ?s fcrepo:hasVersion ?v .
            ?v ?p ?o .
          }
        }
        '''
        try:
            rsp = self.ds.query(q, initBindings={'s': urn})
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            rsp = Graph()

        if not len(rsp):
            raise ResourceNotExistsError(
                    urn, 'No version found for this resource.')
        else:
            return rsp.graph


    def get_version(self, urn, ver_uid):
        '''
        See base_rdf_layout.get_version.
        '''
        q = '''
        CONSTRUCT {
          ?v ?p ?o .
        } WHERE {
          GRAPH fcg:metadata {
            ?s fcrepo:hasVersion ?v .
            ?v fcrepo:hasVersionLabel ?uid .
          }
          GRAPH fcg:historic {
            ?v ?p ?o .
          }
        }
        '''
        try:
            rsp = self.ds.query(q, initBindings={
                's': urn, 'uid': Literal(ver_uid)})
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            rsp = Graph()

        if not len(rsp):
            raise ResourceNotExistsError(
                urn,
                'No version found for this resource with the given label.')
        else:
            return rsp.graph


    def modify_dataset(self, remove_trp=Graph(), add_trp=Graph(),
            types={nsc['fcrepo'].Resource}):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        #self._logger.debug('Remove triples: {}'.format(pformat(
        #        set(remove_trp))))
        #self._logger.debug('Add triples: {}'.format(pformat(
        #        set(add_trp))))

        if not types:
            # @FIXME This is terrible, but I can't get Fuseki to update the
            # default graph without using a variable.
            #target_gr = self.ds.graph(self.UNION_GRAPH_URI)
            target_gr = {
                self.ds.graph(self.HIST_GRAPH_URI),
                self.ds.graph(self.META_GRAPH_URI),
                self.ds.graph(self.MAIN_GRAPH_URI),
            }
        elif nsc['fcrepo'].Metadata in types:
            target_gr = {self.ds.graph(self.META_GRAPH_URI)}
        elif nsc['fcrepo'].Version in types:
            target_gr = {self.ds.graph(self.HIST_GRAPH_URI)}
        else:
            target_gr = {self.ds.graph(self.MAIN_GRAPH_URI)}

        for gr in target_gr:
            gr -= remove_trp
            gr += add_trp
        #for t in remove_trp:
        #    target_gr.remove(t)
        #for t in add_trp:
        #    target_gr.add(t)
