import logging

from collections import defaultdict

from flask import g
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import Literal

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.exceptions import (InvalidResourceError,
        ResourceNotExistsError, TombstoneError, PathSegmentError)
from lakesuperior.store_layouts.ldp_rs.lmdb_store import TxnManager


META_GR_URI = nsc['fcsystem']['meta']
HIST_GR_URI = nsc['fcsystem']['histmeta']
PTREE_GR_URI = nsc['fcsystem']['pairtree']
VERS_CONT_LABEL = 'fcr:versions'


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
    _graph_uids = ('fcadmin', 'fcmain', 'fcstruct')

    # @TODO Move to a config file?
    attr_map = {
        nsc['fcadmin']: {
            # List of server-managed predicates. Triples bearing one of these
            # predicates will go in the metadata graph.
            'p': {
                nsc['fcrepo'].created,
                nsc['fcrepo'].createdBy,
                nsc['fcrepo'].hasParent,
                nsc['fcrepo'].hasVersion,
                nsc['fcrepo'].lastModified,
                nsc['fcrepo'].lastModifiedBy,
                nsc['fcsystem'].tombstone,
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
                nsc['fcsystem'].Tombstone,
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

    # RDF types of graphs by prefix.
    graph_ns_types = {
        nsc['fcadmin']: nsc['fcsystem'].AdminGraph,
        nsc['fcmain']: nsc['fcsystem'].UserProvidedGraph,
        nsc['fcstruct']: nsc['fcsystem'].StructureGraph,
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

        The output of this is a dict with a similar structure:

        {
            'p': {
                <Predicate P1>: <destination graph G1>,
                <Predicate P2>: <destination graph G1>,
                <Predicate P3>: <destination graph G1>,
                <Predicate P4>: <destination graph G2>,
                [...]
            },
            't': {
                <RDF Type T1>: <destination graph G1>,
                <RDF Type T2>: <destination graph G3>,
                [...]
            }
        }
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
        store = self.ds.store
        if getattr(store, 'is_txn_open', False):
            store.rollback()
        store.destroy(store.path)

        self._logger.info('Initializing the graph store with system data.')
        store.open(store.path)
        with TxnManager(store, True):
            with open('data/bootstrap/rsrc_centric_layout.sparql', 'r') as f:
                self.ds.update(f.read())


    def get_raw(self, uri, ctx):
        '''
        Get a raw graph of a non-LDP resource.

        The graph is queried across all contexts or within a specific one.

        @param s(rdflib.term.URIRef) URI of the subject.
        @param ctx (rdflib.term.URIRef) URI of the optional context. If None,
        all named graphs are queried.

        return rdflib.Graph
        '''
        bindings = {'s': uri}
        if ctx:
            bindings['g'] = ctx

        qry = '''
        CONSTRUCT { ?s ?p ?o . } {
          GRAPH ?g {
            ?s ?p ?o .
          }
        }'''

        return self._parse_construct(qry, init_bindings=bindings)


    def raw_query(self, qry_str):
        '''
        Perform a straight query to the graph store.
        '''
        return self.ds.query(qry_str)


    def extract_imr(
                self, uid, ver_uid=None, strict=True, incl_inbound=False,
                incl_children=True, embed_children=False, **kwargs):
        '''
        See base_rdf_layout.extract_imr.
        '''
        if ver_uid:
            uid = self.snapshot_uid(uid, ver_uid)
        if incl_children:
            incl_child_qry = ''
            if embed_children:
                pass # Not implemented. May never be.
        else:
            incl_child_qry = (
                '\n FILTER NOT EXISTS { ?g a fcsystem:StructureGraph . }')

        qry = '''
        CONSTRUCT {{?s ?p ?o . }}
        WHERE {{
          GRAPH fcsystem:meta {{
            ?g foaf:primaryTopic ?rsrc .
            {}
          }}
          GRAPH ?g {{ ?s ?p ?o . }}
        }}'''.format(incl_child_qry)

        gr = self._parse_construct(qry, init_bindings={
            'rsrc': nsc['fcres'][uid],
        })

        if incl_inbound and len(gr):
            gr += self.get_inbound_rel(nsc['fcres'][uid])

        #self._logger.debug('Found resource: {}'.format(
        #        gr.serialize(format='turtle').decode('utf-8')))
        rsrc = Resource(gr, nsc['fcres'][uid])

        if strict:
            self._check_rsrc_status(rsrc)

        return rsrc


    def ask_rsrc_exists(self, uid):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        meta_gr = self.ds.graph(nsc['fcadmin'][uid])
        return bool(
                meta_gr[nsc['fcres'][uid] : RDF.type : nsc['fcrepo'].Resource])


    def get_metadata(self, uid, ver_uid=None, strict=True):
        '''
        This is an optimized query to get only the administrative metadata.
        '''
        if ver_uid:
            uid = self.snapshot_uid(uid, ver_uid)
        gr = self.ds.graph(nsc['fcadmin'][uid]) | Graph()
        if not len(gr):
            # If no resource is found, search in pairtree graph.
            try:
                gr = self.ds.graph(PTREE_GR_URI).query(
                        'CONSTRUCT WHERE {?s ?p ?o}',
                        initBindings={'s': nsc['fcres'][uid]}).graph
            except ResultException:
                gr = Graph()

        rsrc = Resource(gr, nsc['fcres'][uid])
        if strict:
            self._check_rsrc_status(rsrc)

        return rsrc


    def get_version_info(self, uid, strict=True):
        '''
        Get all metadata about a resource's versions.
        '''
        # @NOTE This pretty much bends the ontology—it replaces the graph URI
        # with the subject URI. But the concepts of data and metadata in Fedora
        # are quite fluid anyways...
        qry = '''
        CONSTRUCT {
          ?s fcrepo:hasVersion ?v .
          ?v ?p ?o .
        } {
          GRAPH ?ag {
            ?s fcrepo:hasVersion ?v .
          }
          GRAPH ?hg {
            ?vm foaf:primaryTopic ?v .
            ?vm  ?p ?o .
            FILTER (?o != ?v)
          }
        }'''
        gr = self._parse_construct(qry, init_bindings={
            'ag': nsc['fcadmin'][uid],
            'hg': HIST_GR_URI,
            's': nsc['fcres'][uid]})
        rsrc = Resource(gr, nsc['fcres'][uid])
        if strict:
            self._check_rsrc_status(rsrc)

        return rsrc


    def get_inbound_rel(self, uri):
        '''
        Query inbound relationships for a subject.

        @param subj_uri Subject URI.
        '''
        #import pdb; pdb.set_trace()
        # Only search in non-historic graphs.
        qry = '''
        CONSTRUCT { ?s1 ?p1 ?s }
        WHERE {
          GRAPH ?g {
            ?s1 ?p1 ?s .
          }
          GRAPH ?mg {
            ?g foaf:primaryTopic ?s1 .
          }
        }
        '''
        return self._parse_construct(qry, init_bindings={'s': uri})


    def get_recursive(self, uid, predicate):
        '''
        Get recursive references for a resource predicate.

        @param uid (stirng) Resource UID.
        @param predicate (URIRef) Predicate URI.
        '''
        ds = self.ds
        uri = nsc['fcres'][uid]
        def recurse(dset, s, p):
            new_dset = set(ds[s : p])
            for ss in new_dset:
                dset.add(ss)
                if set(ds[ss : p]):
                    recurse(dset, ss, p)
            return dset

        return recurse(set(), uri, predicate)


    def patch_rsrc(self, uid, qry):
        '''
        Patch a resource with SPARQL-Update statements.

        The statement(s) is/are executed on the user-provided graph only
        to ensure that the scope is limited to the resource.

        @param uid (string) UID of the resource to be patched.
        @param qry (dict) Parsed and translated query, or query string.
        '''
        # Add meta graph for user-defined triples. This may not be used but
        # it's simple and harmless to add here.
        self.ds.graph(META_GR_URI).add(
                (nsc['fcmain'][uid], nsc['foaf'].primaryTopic,
                nsc['fcres'][uid]))
        gr = self.ds.graph(nsc['fcmain'][uid])
        self._logger.debug('Updating graph {} with statements: {}'.format(
            nsc['fcmain'][uid], qry))

        return gr.update(qry)


    def purge_rsrc(self, uid, inbound=True, backup_uid=None):
        '''
        Completely delete a resource and (optionally) its references.
        '''
        qry = '''
        DELETE WHERE {{
          GRAPH ?g {{ {rsrc} fcrepo:hasVersion ?v . }}
          GRAPH {histmeta} {{
            ?vg foaf:primaryTopic ?v ;
              ?gp ?go .
          }}
          GRAPH ?vg {{ ?vs ?vp ?vo }}
        }}
        ;
        DELETE WHERE {{
          GRAPH {meta} {{
            ?g foaf:primaryTopic {rsrc} ;
              ?gp ?go .
          }}
          GRAPH ?g {{ ?s ?p ?o . }}
        }}
        '''.format(rsrc=nsc['fcres'][uid].n3(),
            meta=META_GR_URI.n3(), histmeta=HIST_GR_URI.n3())

        self.ds.update(qry)

        if inbound:
            # Gather ALL subjects in the user graph. There may be fragments.
            # Do not delete inbound references from historic graphs.
            qry = '''
            DELETE {{ GRAPH ?ibg {{ ?ibs ?ibp ?s . }} }}
            WHERE {{
              GRAPH {ug} {{ ?s ?p ?o . }}
              GRAPH ?ibg {{ ?ibs ?ibp ?s . }}
              GRAPH {mg} {{ ?ibg foaf:primaryTopic ?ibs . }}
            }}'''.format(
            mg=META_GR_URI.n3(),
            ug=nsc['fcmain'][uid].n3())

            self.ds.update(qry)


    def create_or_replace_rsrc(self, uid, trp):
        '''
        Create a new resource or replace an existing one.
        '''
        self.delete_rsrc_data(uid)

        return self.modify_rsrc(uid, add_trp=trp)


    def modify_rsrc(self, uid, remove_trp=set(), add_trp=set()):
        '''
        Modify triples about a subject.

        This method adds and removes triple sets from specific graphs,
        indicated by the term router. It also adds metadata about the changed
        graphs.
        '''
        remove_routes = defaultdict(set)
        add_routes = defaultdict(set)
        historic = VERS_CONT_LABEL in uid

        graph_types = set() # Graphs that need RDF type metadata added.
        # Create add and remove sets for each graph.
        for t in remove_trp:
            map_graph = self._map_graph_uri(t, uid)
            target_gr_uri = map_graph[0]
            remove_routes[target_gr_uri].add(t)
            graph_types.add(map_graph)
        for t in add_trp:
            map_graph = self._map_graph_uri(t, uid)
            target_gr_uri = map_graph[0]
            add_routes[target_gr_uri].add(t)
            graph_types.add(map_graph)

        # Decide if metadata go into historic or current graph.
        meta_gr_uri = HIST_GR_URI if historic else META_GR_URI
        meta_gr = self.ds.graph(meta_gr_uri)

        # Remove and add triple sets from each graph.
        for gr_uri, trp in remove_routes.items():
            gr = self.ds.graph(gr_uri)
            gr -= trp
        for gr_uri, trp in add_routes.items():
            gr = self.ds.graph(gr_uri)
            gr += trp
            # Add metadata.
            meta_gr.set(
                    (gr_uri, nsc['foaf'].primaryTopic, nsc['fcres'][uid]))
            meta_gr.set((gr_uri, nsc['fcrepo'].created, g.timestamp_term))
            if historic:
                # @FIXME Ugly reverse engineering.
                ver_uid = uid.split(VERS_CONT_LABEL)[1].lstrip('/')
                meta_gr.set((
                    gr_uri, nsc['fcrepo'].hasVersionLabel, Literal(ver_uid)))
            # @TODO More provenance metadata can be added here.

        # Add graph RDF types.
        for gr_uri, gr_type in graph_types:
            meta_gr.add((gr_uri, RDF.type, gr_type))


    def delete_rsrc_data(self, uid):
        for guid in self._graph_uids:
            self.ds.remove_graph(self.ds.graph(nsc[guid][uid]))


    def snapshot_uid(self, uid, ver_uid):
        '''
        Create a versioned UID string from a main UID and a version UID.
        '''
        if VERS_CONT_LABEL in uid:
            raise InvalidResourceError(uid,
                    'Resource \'{}\' is already a version.')

        return '{}/{}/{}'.format(uid, VERS_CONT_LABEL, ver_uid)


    def add_path_segment(self, uid, next_uid, parent_uid, child_uid):
        '''
        Add a pairtree segment.

        @param uid (string) The UID of the subject.
        @param next_uid (string) UID of the next step down. This may be an LDP
        resource or another segment.
        @param parent_uid (string) UID of the actual resource(s) that contains
        the segment.
        @param child_uid (string) UID of the LDP resource contained by the
        segment.
        '''
        props = (
            (RDF.type, nsc['fcsystem'].PathSegment),
            (nsc['fcsystem'].contains, nsc['fcres'][next_uid]),
            (nsc['ldp'].contains, nsc['fcres'][child_uid]),
            #(RDF.type, nsc['ldp'].Container),
            #(RDF.type, nsc['ldp'].BasicContainer),
            #(RDF.type, nsc['ldp'].RDFSource),
            #(RDF.type, nsc['fcrepo'].Pairtree),
            (nsc['fcrepo'].hasParent, nsc['fcres'][parent_uid]),
        )
        for p, o in props:
            self.ds.graph(PTREE_GR_URI).add((nsc['fcres'][uid], p, o))


    def delete_path_segment(self, uid):
        '''
        Delete a pairtree segment.
        '''
        self.ds.graph(PTREE_GR_URI).delete((nsc['fcres'][uid], None, None))


    def clear_smt(self, uid):
        '''
        This is an ugly way to deal with lenient SPARQL update statements
        that may insert server-managed triples into a user graph.

        @TODO Deprecate when a solution to provide a sanitized SPARQL update
        sring is found.
        '''
        gr = self.ds.graph(nsc['fcmain'][uid])
        for p in srv_mgd_predicates:
            gr.remove((None, p, None))
        for t in srv_mgd_types:
            gr.remove((None, RDF.type, t))


    ## PROTECTED MEMBERS ##

    def _check_rsrc_status(self, rsrc):
        '''
        Check if a resource is not existing or if it is a tombstone.
        '''
        uid = g.tbox.uri_to_uuid(rsrc.identifier)
        if not len(rsrc.graph):
            raise ResourceNotExistsError(uid)

        # Check if resource is a tombstone.
        if rsrc[RDF.type : nsc['fcsystem'].Tombstone]:
            raise TombstoneError(
                    uid, rsrc.value(nsc['fcrepo'].created))
        elif rsrc.value(nsc['fcsystem'].tombstone):
            raise TombstoneError(
                    g.tbox.uri_to_uuid(
                        rsrc.value(nsc['fcsystem'].tombstone).identifier),
                        rsrc.value(nsc['fcrepo'].created))


    def _parse_construct(self, qry, init_bindings={}):
        '''
        Parse a CONSTRUCT query and return a Graph.
        '''
        try:
            qres = self.ds.query(qry, initBindings=init_bindings)
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            return Graph()
        else:
            return qres.graph


    def _map_graph_uri(self, t, uid):
        '''
        Map a triple to a namespace prefix corresponding to a graph.

        @return Tuple with a graph URI and an associated RDF type.
        '''
        if t[1] in self.attr_routes['p'].keys():
            pfx = self.attr_routes['p'][t[1]]
        elif t[1] == RDF.type and t[2] in self.attr_routes['t'].keys():
            pfx = self.attr_routes['t'][t[2]]
        else:
            pfx = nsc['fcmain']

        return (pfx[uid], self.graph_ns_types[pfx])
