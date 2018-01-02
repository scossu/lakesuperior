import logging

from collections import defaultdict
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

    attr_map = {
        nsc['fcadmin']: {
            # List of server-managed predicates. Triples bearing one of these
            # predicates will go in the metadata graph.
            'p': {
                nsc['fcrepo'].created,
                nsc['fcrepo'].createdBy,
                nsc['fcrepo'].hasParent,
                nsc['fcrepo'].lastModified,
                nsc['fcrepo'].lastModifiedBy,
                # The following 3 are set by the user but still in this group
                # for convenience.
                nsc['ldp'].membershipResource,
                nsc['ldp'].hasMemberRelation,
                nsc['ldp'].insertedContentRelation,
                nsc['iana'].describedBy,
                nsc['premis'].hasMessageDigest,
                nsc['premis'].hasSize,
            },
            # List of metadata RDF types. Triples bearing one of these types in
            # the object will go in the metadata graph.
            't': {
                nsc['fcrepo'].Binary,
                nsc['fcrepo'].Container,
                nsc['fcrepo'].Pairtree,
                nsc['fcrepo'].Resource,
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
            # These are placed in a separate graph for optimization purposes.
            'p': {
                nsc['fcsystem'].contains,
                nsc['ldp'].contains,
                nsc['pcdm'].hasMember,
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
        special meaning to the application) go to the `fcmain:` graph.
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
        #self.ds.default_context.parse(
        #        source='data/bootstrap/rsrc_centric_layout.nq', format='nquads')
        with open('data/bootstrap/rsrc_centric_layout.sparql', 'r') as f:
            self.ds.update(f.read())

        self.ds.store.commit()
        self.ds.store.close()


    def extract_imr(
                self, uid, ver_uid=None, strict=True, incl_inbound=False,
                incl_children=True, embed_children=False):
        '''
        See base_rdf_layout.extract_imr.
        '''
        # @TODO Remove inbound functionality in favor of SPARQL query endpoint?
        #inbound_construct = '\n?s1 ?p1 ?s .' if incl_inbound else ''
        #inbound_qry = '''
        #UNION {
        #  GRAPH ?g {
        #    ?s1 ?p1 ?s .
        #  }
        #  GRAPH ?mg {
        #    ?g a fcsystem:CurrentState .
        #  }
        #}
        #''' if incl_inbound else ''
        mg = self._admin_uri(uid, ver_uid)
        strg = nsc['fcstruct'][uid]
        sg = self._main_uri(uid, ver_uid)

        if incl_children:
            incl_child_qry = 'FROM {}'.format(strg.n3())
            if embed_children:
                pass # Not implemented. May never be.
        else:
            incl_child_qry = ''

        q = '''
        CONSTRUCT
        FROM {mg}
        FROM {sg}
        {chld}
        WHERE {{ ?s ?p ?o . }}
        '''.format(mg=mg.n3(), sg=sg.n3(), chld=incl_child_qry)
        try:
            qres = self.ds.query(q, initBindings={'mg': mg, 'strg': strg,
                'sg': sg})
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
        meta_gr = self.ds.graph(self._admin_uri(uid))
        return bool(
                meta_gr[nsc['fcres'][uid] : RDF.type : nsc['fcrepo'].Resource])


    def get_metadata(self, uid, ver_uid=None):
        '''
        This is an optimized query to get everything the application needs to
        insert new contents, and nothing more.
        '''
        gr = self.ds.graph(self._admin_uri(uid, ver_uid)) | Graph()

        return Resource(gr, nsc['fcres'][uid])


    def create_snapshot(self, uid, ver_uid):
        '''
        Create a version snapshot.
        '''
        state_gr = self.ds.graph(self._main_uri(uid))
        state_ver_gr = self.ds.graph(self._main_uri(uid, ver_uid))
        meta_gr = self.ds.graph(self._admin_uri(uid))
        meta_ver_gr = self.ds.graph(self._admin_uri(uid, ver_uid))




    def get_version(self, uid, ver_uid):
        '''
        See base_rdf_layout.get_version.
        '''
        # @TODO
        gr = self.ds.graph(self._main_uri(uid, ver_uid))
        return Resource(gr | Graph(), nsc['fcres'][uid])


    def create_or_replace_rsrc(self, uid, trp, ver_uid=None):
        '''
        Create a new resource or replace an existing one.
        '''
        sg_uri = self._main_uri(uid)
        mg_uri = self._admin_uri(uid)
        if ver_uid:
            ver_uri = self._main_uri(uid, ver_uid)
            drop_qry = 'MOVE SILENT {sg} TO {vg};\n'.format(
                    sg=sg_uri.n3(), vg=ver_uri.n3())
        else:
            drop_qry = 'DROP SILENT GRAPH {};\n'.format(sg_uri.n3())
        drop_qry += 'DROP SILENT GRAPH {}\n'.format(mg_uri.n3())

        self.ds.update(drop_qry)

        return self.modify_rsrc(uid, add_trp=trp)
        #sg = self.ds.graph(sg_uri)
        #sg += data
        #mg = self.ds.graph(mg_uri)
        #mg += metadata


    def modify_rsrc(self, uid, remove_trp=set(), add_trp=set()):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        remove_routes = defaultdict(set)
        add_routes = defaultdict(set)

        # Create add and remove sets for each graph.
        for t in remove_trp:
            target_gr_uri = self._map_graph_uri(t, uid)
            remove_routes[target_gr_uri].add(t)
        for t in add_trp:
            target_gr_uri = self._map_graph_uri(t, uid)
            add_routes[target_gr_uri].add(t)

        # Remove and add triple sets from each graph.
        for gr_uri, trp in remove_routes.items():
            gr = self.ds.graph(gr_uri)
            gr -= trp
        for gr_uri, trp in add_routes.items():
            gr = self.ds.graph(gr_uri)
            gr += trp


    ## PROTECTED MEMBERS ##

    def _main_uri(self, uid, ver_uid=None):
        '''
        Convert a UID into a request URL to the graph store.
        '''
        if ver_uid:
            uid += ':' + ver_uid

        return nsc['fcmain'][uid]


    def _admin_uri(self, uid, ver_uid=None):
        '''
        Convert a UID into a request URL to the graph store.
        '''
        if ver_uid:
            uid += ':' + ver_uid

        return nsc['fcadmin'][uid]


    def _map_graph_uri(self, t, uid):
        '''
        Map a triple to a namespace prefix corresponding to a graph.
        '''
        if t[1] in self.attr_routes['p'].keys():
            return self.attr_routes['p'][t[1]][uid]
        elif t[1] == RDF.type and t[2] in self.attr_routes['t'].keys():
            return self.attr_routes['t'][t[2]][uid]
        else:
            return nsc['fcmain'][uid]
