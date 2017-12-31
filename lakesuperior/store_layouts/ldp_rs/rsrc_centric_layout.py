import logging

from copy import deepcopy
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
from lakesuperior.model.ldpr import ROOT_UID, ROOT_GRAPH_URI, ROOT_RSRC_URI


class RsrcCentricLayout:
    '''
    This class exposes an interface to build graph store layouts. It also
    provides the basics of the triplestore connection.

    Some store layouts are provided. New ones aimed at specific uses
    and optimizations of the repository may be developed by extending this
    class and implementing all its abstract methods.

    A layout is implemented via application configuration. However, once
    contents are ingested in a repository, changing a layout will most likely
    require a migration.

    The custom layout must be in the lakesuperior.store_layouts.rdf
    package and the class implementing the layout must be called
    `StoreLayout`. The module name is the one defined in the app
    configuration.

    E.g. if the configuration indicates `simple_layout` the application will
    look for
    `lakesuperior.store_layouts.rdf.simple_layout.SimpleLayout`.

    Some method naming conventions:

    - Methods starting with `get_` return a resource.
    - Methods starting with `list_` return an iterable or generator of URIs.
    - Methods starting with `select_` return an iterable or generator with
      table-like data such as from a SELECT statement.
    - Methods starting with `ask_` return a boolean value.
    '''

    _logger = logging.getLogger(__name__)

    META_GRAPH_URI = nsc['fcsystem'].meta

    attr_map = {
        nsc['fcmeta']: {
            # List of metadata predicates. Triples bearing one of these
            # predicates will go in the metadata graph.
            'p': {
                nsc['fcrepo'].created,
                nsc['fcrepo'].createdBy,
                nsc['fcrepo'].lastModified,
                nsc['fcrepo'].lastModifiedBy,
                nsc['premis'].hasMessageDigest,
            },
            # List of metadata RDF types. Triples bearing one of these types in
            # the object will go in the metadata graph.
            't': {
                nsc['fcrepo'].Binary,
                nsc['fcrepo'].Container,
                nsc['fcrepo'].Pairtree,
                nsc['ldp'].BasicContainer,
                nsc['ldp'].Container,
                nsc['ldp'].DirectContainer,
                nsc['ldp'].IndirectContainer,
                nsc['ldp'].NonRDFSource,
                nsc['ldp'].RDFSource,
                nsc['ldp'].Resource,
            },
        },
        nsc['fcstruct']: {
            # These are placed in a separate graph for optimization purposees.
            'p': {
                nsc['fcrepo'].hasParent,
                nsc['fcsystem'].contains,
                nsc['ldp'].contains,
            }
        },
    }


    ## MAGIC METHODS ##

    def __init__(self, conn, config):
        '''Initialize the graph store and a layout.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). Blazegraph supports
        this only in the (currently unreleased) 2.2 branch. It works with Jena,
        which is currently the reference implementation.
        '''
        self.config = config
        self._conn = conn
        self.store = self._conn.store

        #self.UNION_GRAPH_URI = self._conn.UNION_GRAPH_URI
        self.ds = self._conn.ds
        self.ds.namespace_manager = nsm


    @property
    def attr_routes(self):
        '''
        This is a map that allows specific triples to go to certain graphs.
        It is a machine-friendly version of the static attribute `attr_map`
        which is formatted for human readability and to avoid repetition.
        The attributes not mapped here (usually user-provided triples with no
        special meaning to the application) go to the `fcstate:` graph.
        '''
        if not hasattr(self, '_attr_routes'):
            self._attr_routes = {'p': {}, 't': {}}
            for dest in self.attr_map.keys():
                for term_k, terms in self.attr_map[dest].items():
                    self._attr_routes[term_k].update(
                            {term: dest for term in terms})

        return self._attr_routes



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
            incl_children_qry = '''
            UNION {
              GRAPH ?strg {
                ?str_s ?str_p ?str_o .
              }
            }
            '''

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
            incl_children_qry = ''

        q = '''
        CONSTRUCT {{
            ?meta_s ?meta_p ?meta_o .
            ?s ?p ?o .{inb_cnst}
            ?str_s ?str_p ?str_o .
            {embed_chld_t}
            #?s fcrepo:writable true .
        }}
        WHERE {{
          {{
            GRAPH ?mg {{
              ?meta_s ?meta_p ?meta_o .
            }}
          }}{incl_chld}{embed_chld}
          UNION {{
            GRAPH ?sg {{
              ?s ?p ?o .{inb_qry}
            }}
          }}{inb_qry}
        }}
        '''.format(
                inb_cnst=inbound_construct, inb_qry=inbound_qry,
                incl_chld=incl_children_qry, embed_chld_t=embed_children_trp,
                embed_chld=embed_children_qry,
                )

        mg = ROOT_GRAPH_URI if uid == '' else nsc['fcmeta'][uid]
        strg = ROOT_GRAPH_URI if uid == '' else nsc['fcstruct'][uid]
        try:
            qres = self.ds.query(q, initBindings={'mg': mg, 'strg': strg,
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
        mg_uri = ROOT_GRAPH_URI if uid == '' else nsc['fcmeta'][uid]
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


    def modify_rsrc(self, uid, remove_trp=set(), add_trp=set()):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        for t in remove_trp:
            target_gr = self.ds.graph(self._map_graph_uri(t, uid))
            target_gr.remove(t)

        for t in add_trp:
            target_gr = self.ds.graph(self._map_graph_uri(t, uid))
            target_gr.add(t)


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


    def _map_graph_uri(self, t, uid):
        '''
        Map a triple to a namespace prefix corresponding to a graph.
        '''
        if not uid:
            return ROOT_GRAPH_URI

        if t[1] in self.attr_routes['p'].keys():
            return self.attr_routes['p'][t[1]][uid]
        elif t[1] == RDF.type and t[2] in self.attr_routes['t'].keys():
            return self.attr_routes['t'][t[2]][uid]
        else:
            return nsc['fcstate'][uid]
