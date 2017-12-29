import logging

from copy import deepcopy
from pprint import pformat
from urllib.parse import quote

import requests

from flask import current_app
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.dictionaries.namespaces import ns_pfx_sparql
from lakesuperior.exceptions import (InvalidResourceError, InvalidTripleError,
        ResourceNotExistsError, TombstoneError)
from lakesuperior.store_layouts.ldp_rs.base_rdf_layout import BaseRdfLayout
from lakesuperior.model.ldpr import ROOT_UID, ROOT_GRAPH_URI, ROOT_RSRC_URI


class RsrcCentricLayout(BaseRdfLayout):
    '''
    Resource-centric layout.

    See http://patterns.dataincubator.org/book/graph-per-resource.html
    This implementation places each resource and its fragments within a named
    graph. Version snapshots are also stored in individual graphs and are named
    related in a metadata graph.

    This layout is best used not with a connector that uses RDFlib but rather
    with one that employs a direct interaction with the Graph Store Protocol,
    either via HTTP or, ideally, using native API bindings.
    '''
    _logger = logging.getLogger(__name__)

    META_GRAPH_URI = nsc['fcsystem'].meta

    def bootstrap(self):
        '''
        Delete all graphs and insert the basic triples.
        '''
        self._logger.info('Deleting all data from the graph store.')
        self.ds.update('DROP SILENT ALL')

        self._logger.info('Initializing the graph store with system data.')
        self.ds.default_context.parse(
                source='data/bootstrap/rsrc_centric_layout.nq', format='nquads')

        self.ds.store.close()


    def extract_imr(
                self, uid, ver_uid=None, strict=True, incl_inbound=False,
                incl_children=True, embed_children=False, incl_srv_mgd=True):
        '''
        See base_rdf_layout.extract_imr.
        '''
        inbound_construct = '\n?s1 ?p1 ?s .' if incl_inbound else ''
        inbound_qry = '''
        OPTIONAL {
          GRAPH ?g {
            ?s1 ?p1 ?s .
          }
          GRAPH ?mg {
            ?g a fcsystem:CurrentState .
          }
        }
        ''' if incl_inbound else ''

        # Include and/or embed children.
        embed_children_trp = embed_children_qry = ''
        if incl_srv_mgd and incl_children:
            incl_children_qry = ''

            # Embed children.
            if embed_children:
                embed_children_trp = '?c ?cp ?co .'
                embed_children_qry = '''
                UNION {{
                  ?s ldp:contains ?c .
                  {}
                }}
                '''.format(embed_children_trp)
        else:
            incl_children_qry = '\nFILTER ( ?p != ldp:contains )' \

        q = '''
        CONSTRUCT {{
            ?meta_s ?meta_p ?meta_o .
            ?s ?p ?o .{inb_cnst}
            {embed_chld_t}
            #?s fcrepo:writable true .
        }}
        WHERE {{
          {{
            GRAPH ?mg {{
              ?meta_s ?meta_p ?meta_o .
            }}
          }} UNION {{
            GRAPH ?sg {{
              ?s ?p ?o .{inb_qry}{incl_chld}{embed_chld}
            }}
          }}{inb_qry}
        }}
        '''.format(
                inb_cnst=inbound_construct, inb_qry=inbound_qry,
                incl_chld=incl_children_qry, embed_chld_t=embed_children_trp,
                embed_chld=embed_children_qry,
                )

        mg = ROOT_GRAPH_URI if uid == '' else nsc['fcmeta'][uid]
        #import pdb; pdb.set_trace()
        try:
            qres = self.ds.query(q, initBindings={'mg': mg,
                'sg': self._state_uri(uid, ver_uid)})
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            gr = Graph()
        else:
            gr = qres.graph

        #self._logger.debug('Found resource: {}'.format(
        #        gr.serialize(format='turtle').decode('utf-8')))
        if strict and not len(gr):
            raise ResourceNotExistsError(uid)

        rsrc = Resource(gr, nsc['fcres'][uid])

        # Check if resource is a tombstone.
        if rsrc[RDF.type : nsc['fcsystem'].Tombstone]:
            if strict:
                raise TombstoneError(
                        g.tbox.uri_to_uuid(rsrc.identifier),
                        rsrc.value(nsc['fcrepo'].created))
            else:
                self._logger.info('Tombstone found: {}'.format(uid))
        elif rsrc.value(nsc['fcsystem'].tombstone):
            if strict:
                raise TombstoneError(
                        g.tbox.uri_to_uuid(
                            rsrc.value(nsc['fcsystem'].tombstone).identifier),
                        rsrc.value(nsc['fcrepo'].created))
            else:
                self._logger.info('Parent tombstone found: {}'.format(uri))

        return rsrc


    def ask_rsrc_exists(self, uid):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        meta_gr = self.ds.graph(nsc['fcmeta'][uid])
        return bool(
                meta_gr[nsc['fcres'][uid] : RDF.type : nsc['fcrepo'].Resource])


    def get_metadata(self, uid):
        '''
        This is an optimized query to get everything the application needs to
        insert new contents, and nothing more.
        '''
        rsrc_uri = nsc['fcres'][uid]
        meta_uri = ROOT_GRAPH_URI if uid == ROOT_UID else nsc['fcmeta'][uid]
        state_uri = ROOT_GRAPH_URI if uid == ROOT_UID else nsc['fcmeta'][uid]
        meta_gr = self.ds.graph(meta_uri)
        state_gr = self.ds.graph(state_uri)
        cont_qry = '''
        CONSTRUCT {
          ?s ldp:membershipResource ?mr ;
            ldp:hasMemberRelation ?hmr ;
            ldp:insertedContentRelation ?icr ;
              ?p ?o .
        } WHERE {
          {
            GRAPH ?mg {
              ?s ?p ?o .
            }
          } UNION {
            GRAPH ?sg {
              {
                ?s ldp:membershipResource ?mr ;
                  ldp:hasMemberRelation ?hmr .
              } UNION {
                ?s ldp:insertedContentRelation ?icr .
              }
            }
          }
        }
        '''
        try:
            qres = self.ds.query(cont_qry, initBindings={
                    's': rsrc_uri, 'mg': meta_uri, 'sg': state_uri})
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            gr = Graph()
        else:
            gr = qres.graph

        return Resource(gr, rsrc_uri)


    def get_version(self, uid, ver_uid):
        '''
        See base_rdf_layout.get_version.
        '''
        # @TODO
        gr = self.ds.graph(self._state_uri(uid, ver_uid))
        return Resource(gr | Graph(), nsc['fcres'][uid])


    def create_or_replace_rsrc(self, uid, data, metadata, ver_uid=None):
        '''
        Create a new resource or replace an existing one.
        '''
        sg_uri = self._state_uri(uid)
        mg_uri = self._meta_uri(uid)
        if ver_uid:
            ver_uri = self._state_uri(uid, ver_uid)
            drop_qry = 'MOVE SILENT {sg} TO {vg};\n'.format(
                    sg=sg_uri.n3(), vg=ver_uri.n3())
        else:
            drop_qry = 'DROP SILENT GRAPH {};\n'.format(sg_uri.n3())
        drop_qry += 'DROP SILENT GRAPH {}\n'.format(mg_uri.n3())

        self.ds.update(drop_qry)

        sg = self.ds.graph(sg_uri)
        sg += data
        mg = self.ds.graph(mg_uri)
        mg += metadata


    def modify_dataset(self, uid, remove_trp=set(), add_trp=set(),
            remove_meta=set(), add_meta=set(), **kwargs):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        gr = self.ds.graph(self._state_uri(uid))
        if len(remove_trp):
            gr -= remove_trp
        if len(add_trp):
            gr += add_trp

        meta_gr = self.ds.graph(self._meta_uri(uid))
        if len(remove_meta):
            gr -= remove_meta
        if len(add_meta):
            gr += add_meta


    ## PROTECTED MEMBERS ##

    def _state_uri(self, uid, version_uid=None):
        '''
        Convert a UID into a request URL to the graph store.
        '''
        if not uid:
            #raise InvalidResourceError(uid,
            #        'Repository root does not accept user-defined properties.')
            return ROOT_GRAPH_URI
        if version_uid:
            uid += ':' + version_uid
        else:
            return nsc['fcstate'][uid]


    def _meta_uri(self, uid):
        '''
        Convert a UID into a request URL to the graph store.
        '''
        if not uid:
            return ROOT_GRAPH_URI
        else:
            return nsc['fcmeta'][uid]


    def optimize_edits(self):
        opt_edits = [
                l for l in self.store._edits
                if not l.startswith('PREFIX')]
        #opt_edits = list(ns_pfx_sparql.values()) + opt_edits
        self.store._edits = opt_edits
        self._logger.debug('Changes to be committed: {}'.format(
            pformat(self.store._edits)))

